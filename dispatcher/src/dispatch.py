"""Hand a fast-loop decision to the resolver and do the bookkeeping with its verdict.

Runs in a background task, NOT in the Alertmanager webhook thread: the resolver blocks for the
whole execute+verify cycle (tens of seconds) and Alertmanager gives up on a webhook after ~10 s.
"""
from __future__ import annotations

import json
import logging
from typing import Any

import requests

from attempts import AttemptStore
from guard import IncidentGuard

logger = logging.getLogger("dispatcher")


class ResolverClient:
    def __init__(self, base_url: str, timeout_s: float = 180.0, http: Any = requests,
                 path: str = "/resolve") -> None:
        self._url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
        self._timeout = timeout_s
        self._http = http

    def resolve(self, action_request: dict) -> dict:
        try:
            r = self._http.post(self._url, json=action_request, timeout=self._timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:   # unreachable, timed out, 5xx, bad JSON: nothing is known to have run
            logger.error("resolver call failed: %r", e)
            return {"status": "error", "detail": {"reason": "resolver_unreachable", "error": repr(e)}}


def run_fast_loop(result: dict, resolver: ResolverClient, attempts: AttemptStore,
                  guard: IncidentGuard) -> dict:
    """What the verdict does to the incident state:
         resolved   -> the incident is over: forget the attempts
         unresolved -> an attempt was spent: the next alert for this workload gets the next tier
         error      -> nothing was attempted: the tier does not advance (the alert will retry)"""
    key = result["workload"]
    try:
        verdict = resolver.resolve(result["action_request"])
        status = verdict.get("status")
        if status == "resolved":
            attempts.reset(key)
        elif status == "unresolved":
            attempts.increment(key)
        logger.info("verdict %s", json.dumps({"workload": key, "playbook": result["playbook"],
                                              "attempt": result["action_request"]["context"]["attempt"],
                                              "verdict": verdict}, sort_keys=True))
        return verdict
    finally:
        guard.release(key)


def run_escalation(result: dict, llm_resolver: ResolverClient, attempts: AttemptStore,
                   llm_attempts: AttemptStore, guard: IncidentGuard) -> dict:
    """Same contract as run_fast_loop, but for the LLM agent (table miss).

         resolved   -> incident over: forget BOTH the deterministic and the LLM attempts
         unresolved -> an LLM run was spent: counts against the LLM budget
         error      -> nothing is known to have run: the budget does not advance"""
    key = result["workload"]
    try:
        verdict = llm_resolver.resolve(result["escalation_request"])
        status = verdict.get("status")
        if status == "resolved":
            attempts.reset(key)
            llm_attempts.reset(key)
        elif status == "unresolved":
            llm_attempts.increment(key)
        logger.info("verdict %s", json.dumps({"workload": key, "path": "llm",
                                              "incident": result["escalation_request"]["incident"],
                                              "verdict": verdict}, sort_keys=True))
        return verdict
    finally:
        guard.release(key)


def on_no_match(result: dict) -> None:
    """Nothing to run: deterministic mode, or the LLM budget for this incident is spent."""
    logger.warning("no playbook for workload=%s (%s); giving up",
                   result.get("workload"), result.get("reason"))
