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
from typing import Optional, Protocol

from clock import Clock
from cluster import Cluster

logger = logging.getLogger("llm_resolver.verify")


class Saturation(Protocol):
    def max_cpu_utilisation(self, namespace: str, pods: "list[str]") -> Optional[float]: ...


@dataclass(frozen=True)
class Health:
    rolled_out: bool
    ready: bool
    restarts: int
    pods: tuple = ()     # names of the pods behind the Deployment (for the saturation query)


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
    return Health(rolled_out, ready, restarts, tuple(p["metadata"]["name"] for p in pods))


def wait_until_healthy(cluster: Cluster, namespace: str, name: str, *, timeout_s: float,
                       stable_s: float, poll_s: float, clock: Clock,
                       saturation: Optional[Saturation] = None, max_cpu_util: float = 0.9) -> dict:
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
        util = None
        if last is not None and last.ready and saturation is not None:
            util = saturation.max_cpu_utilisation(namespace, list(last.pods))
        saturated = util is not None and util > max_cpu_util
        if last is not None and last.ready and not saturated:
            if stable_since is None or last.restarts > baseline:
                stable_since, baseline = t, last.restarts   # (re)start the stability clock
            if t - stable_since >= stable_s:
                return {"healthy": True, "waited_s": round(t - start, 1),
                        **({} if util is None else {"max_cpu_utilisation": round(util, 2)})}
        else:
            stable_since = None
        if t - start >= timeout_s:
            return {"healthy": False, "waited_s": round(t - start, 1),
                    "last": None if last is None else
                    {"rolled_out": last.rolled_out, "ready": last.ready, "restarts": last.restarts,
                     **({} if util is None else {"max_cpu_utilisation": round(util, 2)})}}
        clock.sleep(poll_s)
