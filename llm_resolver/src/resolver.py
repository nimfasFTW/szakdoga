"""request -> validate -> agent investigates and fixes -> verify -> verdict.

Same contract as deter_resolver/resolver.py, so the dispatcher can treat both resolvers alike.
The one difference is who decides what to do: there is no `action` in the request, the agent
works it out from the cluster state.

The verdict never trusts the agent. Whatever it reports in its final message, status comes from
verify.wait_until_healthy run on the workload that alerted."""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional, Sequence

from agent import AgentRun
from clock import Clock
from cluster import Cluster
from tools import Toolkit
from verify import wait_until_healthy

logger = logging.getLogger("llm_resolver")

Investigate = Callable[[str, Sequence, int], AgentRun]   # (prompt, tools, max_steps) -> AgentRun


@dataclass(frozen=True)
class ResolverConfig:
    namespaces: frozenset[str]
    stable_s: float = 10.0
    poll_s: float = 2.0
    verify_timeout_s: float = 60.0   # how long the final verification waits after the agent changed something
    max_steps: int = 20              # model calls the agent may use per incident


class ResolveError(Exception):
    """The request cannot be handled. By contract nothing was changed in the cluster."""

    def __init__(self, reason: str, **detail):
        super().__init__(reason)
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True)
class IncidentRequest:
    """What the resolver needs to know. Deliberately a subset of the deterministic resolver's
    request: `action` and `params` are ignored, there is no playbook to execute.

        {"incident": "...",
         "target": {"namespace": "chaos", "name": "uc3-client", "kind": "Deployment", "container": "..."},
         "context": {...anything the caller knows: alertname, pod, attempt, what was already tried...}}
    """
    incident: str
    namespace: str
    kind: str
    name: str
    container: Optional[str] = None
    context: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: dict) -> "IncidentRequest":
        try:
            t = d["target"]
            return cls(incident=d.get("incident", ""), namespace=t["namespace"], name=t["name"],
                       kind=t.get("kind", "Deployment"), container=t.get("container"),
                       context=dict(d.get("context") or {}))
        except (KeyError, TypeError, ValueError) as e:
            raise ResolveError("bad_request", missing_or_invalid=str(e)) from e


def build_prompt(req: IncidentRequest) -> str:
    lines = ["An alert fired. Investigate and fix the problem.", "",
             f"Affected workload: {req.kind} {req.name} in namespace {req.namespace}"]
    if req.container:
        lines.append(f"Container that alerted: {req.container}")
    lines.append(f"All your tools operate on namespace {req.namespace}.")
    if req.context:
        lines += ["", "Context from the alerting pipeline (JSON):",
                  json.dumps(req.context, indent=2, sort_keys=True, default=str)]
    return "\n".join(lines)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def resolve(raw: dict, cluster: Cluster, cfg: ResolverConfig, clock: Clock,
            investigate: Investigate) -> dict:
    """status: 'resolved'   the workload is healthy after the agent's work
               'unresolved' the agent finished (or ran out of steps) and the workload is still not healthy
               'error'      the agent could not do anything at all; the caller must not count it as a tier used"""
    t0, started = clock.now(), _utcnow()
    base = {"incident": raw.get("incident"), "action": "llm_agent"}

    def verdict(status: str, applied: bool = False, applied_after_s=None, **detail) -> dict:
        return {**base, "status": status, "applied": applied, "detail": detail,
                "started_at": started, "finished_at": _utcnow(),
                "action_applied_after_s": applied_after_s, "duration_s": round(clock.now() - t0, 1)}

    try:
        req = IncidentRequest.from_dict(raw)
        if req.namespace not in cfg.namespaces:
            raise ResolveError("namespace_not_allowed", namespace=req.namespace)
        if req.kind != "Deployment":
            raise ResolveError("unsupported_target", kind=req.kind)
    except ResolveError as e:
        return verdict("error", reason=e.reason, **e.detail)

    toolkit = Toolkit(cluster, req.namespace, stable_s=cfg.stable_s, poll_s=cfg.poll_s, clock=clock)
    try:
        run = investigate(build_prompt(req), toolkit.tools, cfg.max_steps)
    except Exception as e:   # could not even start (bad credentials, model init, ...): nothing was attempted
        logger.exception("agent could not start")
        return verdict("error", reason="agent_unavailable", error=repr(e))

    applied = bool(toolkit.changes)
    if run.error and not applied:   # it died before changing anything, so this attempt did not happen
        return verdict("error", reason="agent_failed", error=run.error, steps=run.steps,
                       tool_calls=toolkit.calls)

    # Verify the workload that alerted, whatever the agent says. If it changed something, give the
    # change time to take effect; if it did not (decided it cannot fix it, or the problem went away
    # on its own), just ask whether the workload is healthy right now, i.e. one stability window.
    timeout = cfg.verify_timeout_s if applied else cfg.stable_s + 2 * cfg.poll_s
    result = wait_until_healthy(cluster, req.namespace, req.name, timeout_s=timeout,
                                stable_s=cfg.stable_s, poll_s=cfg.poll_s, clock=clock)
    # the moment the LAST change landed: a fix that needs several steps (e.g. ConfigMap + restart) is in place then
    applied_after = round(toolkit.last_change_at - t0, 1) if toolkit.last_change_at is not None else None

    detail = {
        "summary": run.summary,
        "changes": toolkit.changes,
        "tool_calls": toolkit.calls,
        "llm": {"model": run.model, "steps": run.steps,
                "input_tokens": run.input_tokens, "output_tokens": run.output_tokens},
        "verification": result,
    }
    if run.error:
        detail["agent_error"] = run.error
    return verdict("resolved" if result["healthy"] else "unresolved", applied, applied_after, **detail)
