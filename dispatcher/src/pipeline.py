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
from guard import IncidentGuard
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
                 table: LookupTable, attempts: AttemptStore, guard: IncidentGuard,
                 llm_attempts: AttemptStore | None = None,
                 llm_max_attempts: int = 2) -> dict[str, Any]:
    """Decide what to do about one alert. Never executes anything.

    outcome == "fast_loop": the incident lock for the workload is HELD on return, and the caller
    must hand the result to dispatch.run_fast_loop(), which releases it.
    outcome == "escalate": (only when `llm_attempts` is given) the table missed and the LLM budget
    for this incident is not spent. The lock is HELD on return; the caller must hand the result to
    dispatch.run_escalation(), which releases it.
    Every other outcome leaves the lock free."""
    labels = alert.get("labels", {})
    alertname = labels.get("alertname", "")
    namespace = labels.get("namespace", "")
    pod = labels.get("pod")
    status = alert.get("status")

    def ignored(why: str) -> dict[str, Any]:
        logger.info("ignored alert=%s pod=%s: %s", alertname, pod, why)
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

    key = facts.workload_key
    if not guard.try_acquire(key):
        logger.info("in_flight alert=%s pod=%s workload=%s", alertname, pod, key)
        return {"outcome": "in_flight", "alertname": alertname, "pod": pod, "workload": key,
                "why": "an incident for this workload is already being handled"}

    try:
        attempt = attempts.get(key)
        sig = build_signature(facts, attempt)
        decision = table.match(sig)
    except Exception:
        guard.release(key)
        raise

    pb = decision.playbook
    result = {
        "outcome": "fast_loop" if decision.matched else "no_match",
        "alertname": alertname,
        "pod": pod,
        "workload": key,
        "signature": dataclasses.asdict(sig),
        "playbook": pb.id if pb else None,
        "action": pb.action if pb else None,
        "params": dict(pb.params) if pb else None,
        "reason": decision.reason,
        "facts": dataclasses.asdict(facts),   # restart_count, exit_code, ready, ... for the data lake
    }
    target = {"namespace": facts.namespace, "kind": facts.workload_kind,
              "name": facts.workload_name, "container": facts.container}
    if pb:
        result["action_request"] = {
            "incident": f"{key}#{attempt}",
            "action": pb.action,
            "params": dict(pb.params),
            "target": target,
            "context": {"playbook": pb.id, "alertname": alertname, "pod": pod, "attempt": attempt},
        }
    elif llm_attempts is not None:
        llm_attempt = llm_attempts.get(key)
        if llm_attempt < llm_max_attempts:
            # Table miss, hybrid/LLM mode: keep the lock, the LLM agent works on this incident.
            result["outcome"] = "escalate"
            result["escalation_request"] = {
                "incident": f"{key}#llm{llm_attempt}",
                "target": target,
                "context": {"alertname": alertname, "pod": pod,
                            "attempt": attempt,              # deterministic actions already taken
                            "llm_attempt": llm_attempt,
                            "signature": result["signature"], "facts": result["facts"],
                            "reason": decision.reason},
            }
        else:
            result["reason"] += f"; LLM budget exhausted ({llm_attempt}/{llm_max_attempts})"
            guard.release(key)
    else:
        guard.release(key)   # nothing to execute, so nothing to hold
    logger.info("decision %s", json.dumps(result, sort_keys=True))
    return result
