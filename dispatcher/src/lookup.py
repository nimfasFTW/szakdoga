"""The lookup table: (signature) -> playbook.

Matching rules (all deterministic, no model involved):
  * A playbook constrains a subset of the signature fields. Omitted fields are wildcards.
  * If several playbooks match, the MOST SPECIFIC one (most constrained fields) wins.
  * Two playbooks of equal specificity that could match the same signature are ambiguous
    and rejected when the table is loaded, so a lookup can never be a coin flip.
  * No match is a normal, cheap outcome. It is how "not a known issue" and
    "known issue, but the playbook is exhausted" are both expressed.

This module is shared by the deterministic SUT and the hybrid SUT. What a miss MEANS
differs (deterministic: give up and log; hybrid: escalate to the LLM agent), but that
decision lives in the caller, not here.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional

import yaml

from signature import Signature

MATCH_FIELDS = ("alertname", "waiting_reason", "termination_reason", "attempt")
KNOWN_ACTIONS = ("bump_memory_limit", "wait_and_verify", "scale_deployment")


class TableError(ValueError):
    """The playbook file is invalid. Raised at load time, never during a lookup."""


@dataclass(frozen=True)
class Playbook:
    id: str
    match: Mapping[str, Any]
    action: str
    params: Mapping[str, Any]

    @property
    def specificity(self) -> int:
        return len(self.match)

    def matches(self, sig: Signature) -> bool:
        return all(getattr(sig, field) == value for field, value in self.match.items())

    def can_co_match(self, other: "Playbook") -> bool:
        """True if some signature exists that both playbooks would match."""
        shared = set(self.match) & set(other.match)
        return all(self.match[f] == other.match[f] for f in shared)


@dataclass(frozen=True)
class Decision:
    matched: bool
    reason: str
    playbook: Optional[Playbook] = None


class LookupTable:
    def __init__(self, playbooks: list[Playbook]):
        self._playbooks = list(playbooks)
        self._validate()

    # ---------- loading ----------
    @classmethod
    def from_yaml(cls, path: str | Path) -> "LookupTable":
        with open(path, encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        if not isinstance(raw, dict) or not isinstance(raw.get("playbooks"), list):
            raise TableError("top level must be a mapping with a 'playbooks' list")
        playbooks = []
        for i, entry in enumerate(raw["playbooks"]):
            try:
                playbooks.append(Playbook(
                    id=entry["id"],
                    match=dict(entry["match"]),
                    action=entry["action"],
                    params=dict(entry.get("params") or {}),
                ))
            except (KeyError, TypeError) as e:
                raise TableError(f"playbook #{i}: malformed entry ({e!r})") from e
        return cls(playbooks)

    def _validate(self) -> None:
        ids = [p.id for p in self._playbooks]
        dupes = {i for i in ids if ids.count(i) > 1}
        if dupes:
            raise TableError(f"duplicate playbook ids: {sorted(dupes)}")

        for p in self._playbooks:
            if not p.match:
                raise TableError(f"{p.id}: empty 'match' would catch every alert, "
                                 f"including the ones meant for the LLM")
            unknown = set(p.match) - set(MATCH_FIELDS)
            if unknown:
                raise TableError(f"{p.id}: unknown match field(s) {sorted(unknown)}; "
                                 f"allowed: {MATCH_FIELDS}")
            if "attempt" in p.match:
                a = p.match["attempt"]
                if isinstance(a, bool) or not isinstance(a, int) or a < 0:
                    raise TableError(f"{p.id}: 'attempt' must be an integer >= 0, got {a!r}")
            if p.action not in KNOWN_ACTIONS:
                raise TableError(f"{p.id}: unknown action {p.action!r}; known: {KNOWN_ACTIONS}")

        for i, a in enumerate(self._playbooks):
            for b in self._playbooks[i + 1:]:
                if a.specificity == b.specificity and a.can_co_match(b):
                    raise TableError(f"ambiguous: '{a.id}' and '{b.id}' have equal specificity "
                                     f"and can match the same signature; constrain one of them "
                                     f"further so the winner is explicit (a field set to null "
                                     f"means 'must be absent')")

    # ---------- lookup ----------
    def match(self, sig: Signature) -> Decision:
        candidates = [p for p in self._playbooks if p.matches(sig)]
        if not candidates:
            return Decision(False, f"no playbook matches {sig}")
        best = max(candidates, key=lambda p: p.specificity)
        return Decision(True, f"matched '{best.id}' (specificity {best.specificity})", best)

    @property
    def playbooks(self) -> tuple[Playbook, ...]:
        return tuple(self._playbooks)
