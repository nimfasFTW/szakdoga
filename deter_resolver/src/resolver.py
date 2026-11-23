"""request -> validate -> execute -> verify -> verdict."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from actions import ACTIONS, ActionError, ActionRequest
from clock import Clock
from cluster import Cluster
from saturation import PrometheusSaturation
from verify import wait_until_healthy

logger = logging.getLogger("deter_resolver")


@dataclass(frozen=True)
class ResolverConfig:
    namespaces: frozenset[str]
    stable_s: float = 10.0
    poll_s: float = 2.0
    prometheus_url: Optional[str] = None  
    max_cpu_util: float = 0.9             


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def resolve(raw: dict, cluster: Cluster, cfg: ResolverConfig, clock: Clock) -> dict:
    """status: 'resolved'   the workload is healthy after the action
               'unresolved' an attempt was made (or spent) and it is still not healthy
               'error'      nothing was attempted; the caller must not count it as a tier used"""
    t0, started = clock.now(), _utcnow()
    base = {"incident": raw.get("incident"), "action": raw.get("action")}

    def verdict(status: str, applied: bool = False, applied_after_s=None, **detail) -> dict:
        return {**base, "status": status, "applied": applied, "detail": detail,
                "started_at": started, "finished_at": _utcnow(),
                "action_applied_after_s": applied_after_s, "duration_s": round(clock.now() - t0, 1)}

    try:
        req = ActionRequest.from_dict(raw)
        if req.namespace not in cfg.namespaces:
            raise ActionError("namespace_not_allowed", namespace=req.namespace)
        if req.kind != "Deployment":
            raise ActionError("unsupported_target", kind=req.kind)
        if req.action not in ACTIONS:
            raise ActionError("unknown_action", action=req.action)
        outcome = ACTIONS[req.action](cluster, req)
    except ActionError as e:
        return verdict("error", reason=e.reason, **e.detail)
    except Exception as e:   # e.g. the API refused the patch: by contract nothing was applied
        logger.exception("action failed")
        return verdict("error", reason="action_failed", error=repr(e))

    applied_after = round(clock.now() - t0, 1) if outcome.applied else None
    if outcome.verify_timeout_s is None:
        return verdict("unresolved", outcome.applied, applied_after, **outcome.detail)

    saturation = PrometheusSaturation(cfg.prometheus_url) if cfg.prometheus_url else None
    result = wait_until_healthy(cluster, req.namespace, req.name, timeout_s=outcome.verify_timeout_s,
                                stable_s=cfg.stable_s, poll_s=cfg.poll_s, clock=clock,
                                saturation=saturation, max_cpu_util=cfg.max_cpu_util)
    return verdict("resolved" if result["healthy"] else "unresolved", outcome.applied, applied_after,
                   **outcome.detail, verification=result)
