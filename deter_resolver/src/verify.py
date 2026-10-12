"""Verification: did the action actually fix the workload?

'Healthy' means the Deployment finished rolling out, every replica is ready, and the pods
did not restart for `stable_s` seconds. The stability window matters: a pod that gets
OOM-killed a couple of seconds after start is Ready for a moment between crashes, so a single
'ready' reading would report a false success.

We look at the workload, not at the alert: pod names change on every rollout, and
whether the monitoring has re-evaluated yet should not decide our verdict.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from clock import Clock
from cluster import Cluster

logger = logging.getLogger("deter_resolver.verify")


@dataclass(frozen=True)
class Health:
    rolled_out: bool
    ready: bool
    restarts: int


def _selector(match_labels: dict) -> str:
    return ",".join(f"{k}={v}" for k, v in sorted(match_labels.items()))


def read_health(cluster: Cluster, namespace: str, name: str) -> Health:
    dep = cluster.get_deployment(namespace, name)
    want = dep["spec"].get("replicas", 1)
    st = dep.get("status") or {}
    rolled_out = (
        st.get("observedGeneration", 0) >= dep["metadata"].get("generation", 0)
        and st.get("updatedReplicas", 0) == want
        and st.get("replicas", 0) == want          # no old-revision pods left
    )
    ready = rolled_out and st.get("readyReplicas", 0) == want
    pods = cluster.list_pods(namespace, _selector(dep["spec"]["selector"].get("matchLabels", {})))
    restarts = sum(cs.get("restartCount", 0)
                   for p in pods for cs in (p.get("status", {}).get("containerStatuses") or []))
    return Health(rolled_out, ready, restarts)


def wait_until_healthy(cluster: Cluster, namespace: str, name: str, *, timeout_s: float,
                       stable_s: float, poll_s: float, clock: Clock) -> dict:
    start = clock.now()
    stable_since = None
    baseline = 0
    last: Health | None = None
    while True:
        t = clock.now()
        try:
            last = read_health(cluster, namespace, name)
        except Exception as e:   # a transient API error must not abort the verification
            logger.warning("health read failed: %r", e)
            last = None
        if last is not None and last.ready:
            if stable_since is None or last.restarts > baseline:
                stable_since, baseline = t, last.restarts   # (re)start the stability clock
            if t - stable_since >= stable_s:
                return {"healthy": True, "waited_s": round(t - start, 1)}
        else:
            stable_since = None
        if t - start >= timeout_s:
            return {"healthy": False, "waited_s": round(t - start, 1),
                    "last": None if last is None else
                    {"rolled_out": last.rolled_out, "ready": last.ready, "restarts": last.restarts}}
        clock.sleep(poll_s)
