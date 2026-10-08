"""alert -> (filter) -> enrich -> signature -> lookup -> decision.

No FastAPI and no cluster access in here: the inspector, table and attempt store are
injected, so the whole path can be tested with fakes.
"""
from __future__ import annotations

import dataclasses
import json
import logging
from dataclasses import dataclass
from typing import Any, Protocol

from attempts import AttemptStore
from lookup import LookupTable
from signature import Facts, build_signature

logger = logging.getLogger("dispatcher")


class Inspector(Protocol):
    def inspect(self, alertname: str, namespace: str, pod_name: str,
                container_hint: str | None = None) -> Facts: ...


@dataclass(frozen=True)
class Config:
    namespaces: frozenset[str]
    # Only alerts that are genuinely pod-scoped. Anything else (e.g. KubeDeploymentReplicasMismatch)
    # carries a `pod` label that points at the kube-state-metrics pod, NOT the broken workload.
    pod_alerts: frozenset[str]


def handle_alert(alert: dict, cfg: Config, inspector: Inspector,
                 table: LookupTable, attempts: AttemptStore) -> dict[str, Any]:
    labels = alert.get("labels", {})
    alertname = labels.get("alertname", "")
    namespace = labels.get("namespace", "")
    pod = labels.get("pod")
    status = alert.get("status")

    def ignored(why: str) -> dict[str, Any]:
        logger.debug("ignored alert=%s pod=%s: %s", alertname, pod, why)
        return {"outcome": "ignored", "alertname": alertname, "why": why}

    if status != "firing":
        return ignored(f"status={status}")          # 'resolved' will matter for verification later
    if namespace not in cfg.namespaces:
        return ignored(f"namespace {namespace!r} not watched")
    if alertname not in cfg.pod_alerts:
        return ignored("not a pod-scoped alert this dispatcher handles")
    if not pod:
        return ignored("no pod label")

    facts = inspector.inspect(alertname, namespace, pod, labels.get("container"))
    if not facts.pod_found:
        return {"outcome": "skipped", "alertname": alertname, "pod": pod,
                "why": "pod no longer exists; it will re-alert if the problem persists"}

    attempt = attempts.get(facts.workload_key)
    sig = build_signature(facts, attempt)
    decision = table.match(sig)

    pb = decision.playbook
    result = {
        "outcome": "fast_loop" if decision.matched else "no_match",
        "alertname": alertname,
        "pod": pod,
        "workload": facts.workload_key,
        "signature": dataclasses.asdict(sig),
        "playbook": pb.id if pb else None,
        "action": pb.action if pb else None,
        "params": dict(pb.params) if pb else None,
        "reason": decision.reason,
        "facts": dataclasses.asdict(facts),   # restart_count, exit_code, ready, ... for the data lake
    }
    logger.info("decision %s", json.dumps(result, sort_keys=True))
    return result
