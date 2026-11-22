"""The executors the lookup table can name. Each takes the request and returns an outcome;
none of them verifies anything itself (verify.py does that for all of them)."""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from decimal import ROUND_CEILING, Decimal
from typing import Callable, Optional

from kubernetes.utils import parse_quantity

from cluster import Cluster

MIB = Decimal(2 ** 20)


class ActionError(Exception):
    """The action could not be carried out. By contract nothing was changed in the cluster."""

    def __init__(self, reason: str, **detail):
        super().__init__(reason)
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True)
class ActionRequest:
    incident: str
    action: str
    params: dict
    namespace: str
    kind: str
    name: str
    container: Optional[str] = None
    context: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: dict) -> "ActionRequest":
        try:
            t = d["target"]
            return cls(incident=d.get("incident", ""), action=d["action"], params=dict(d.get("params") or {}),
                       namespace=t["namespace"], kind=t["kind"], name=t["name"],
                       container=t.get("container"), context=dict(d.get("context") or {}))
        except (KeyError, TypeError) as e:
            raise ActionError("bad_request", missing_or_invalid=str(e)) from e


@dataclass(frozen=True)
class ActionOutcome:
    applied: bool                          # did we change anything in the cluster?
    verify_timeout_s: Optional[float]      # None = nothing left to verify, the attempt is spent
    detail: dict = field(default_factory=dict)


def quantity_to_mib(quantity: str) -> int:
    """'100Mi' / '1Gi' / '128M' / '134217728' -> whole MiB, rounded up."""
    return int((Decimal(str(parse_quantity(quantity))) / MIB).to_integral_value(rounding=ROUND_CEILING))


def _pick_container(containers: list[dict], hint: Optional[str]) -> dict:
    if hint:
        for c in containers:
            if c["name"] == hint:
                return c
        raise ActionError("container_not_found", container=hint)
    if len(containers) == 1:
        return containers[0]
    raise ActionError("container_ambiguous", containers=[c["name"] for c in containers])


def bump_memory_limit(cluster: Cluster, req: ActionRequest) -> ActionOutcome:
    pct = float(req.params["increase_pct"])
    cap_mib = int(req.params["max_limit_mib"])

    dep = cluster.get_deployment(req.namespace, req.name)
    container = _pick_container(dep["spec"]["template"]["spec"]["containers"], req.container)
    limit = ((container.get("resources") or {}).get("limits") or {}).get("memory")
    if not limit:
        raise ActionError("no_memory_limit", container=container["name"])

    current = quantity_to_mib(limit)
    new = min(math.ceil(current * (1 + pct / 100)), cap_mib)
    if new <= current:   # already at the pre-approved ceiling: this playbook has nothing left to give
        return ActionOutcome(False, None, {"reason": "ceiling_reached", "current_mib": current,
                                           "max_limit_mib": cap_mib})
    cluster.patch_deployment(req.namespace, req.name, {"spec": {"template": {"spec": {"containers": [
        {"name": container["name"], "resources": {"limits": {"memory": f"{new}Mi"}}}]}}}})
    return ActionOutcome(True, float(req.params.get("timeout_s", 60)),
                         {"container": container["name"], "from_mib": current, "to_mib": new,
                          "capped": new == cap_mib and math.ceil(current * (1 + pct / 100)) > cap_mib})


def scale_deployment(cluster: Cluster, req: ActionRequest) -> ActionOutcome:
    """Add replicas, up to a pre-approved ceiling (the horizontal counterpart of bump_memory_limit)."""
    add = int(req.params["add_replicas"])
    cap = int(req.params["max_replicas"])
    if add < 1:
        raise ActionError("bad_params", add_replicas=add)

    dep = cluster.get_deployment(req.namespace, req.name)
    current = int(dep["spec"].get("replicas", 1))
    new = min(current + add, cap)
    if new <= current:   # already at the pre-approved ceiling: this playbook has nothing left to give
        return ActionOutcome(False, None, {"reason": "ceiling_reached", "current_replicas": current,
                                           "max_replicas": cap})
    cluster.patch_deployment(req.namespace, req.name, {"spec": {"replicas": new}})
    return ActionOutcome(True, float(req.params.get("timeout_s", 90)),
                         {"from_replicas": current, "to_replicas": new, "capped": current + add > cap})


def wait_and_verify(cluster: Cluster, req: ActionRequest) -> ActionOutcome:
    """Deliberately does nothing: gives Kubernetes's own retry/backoff a bounded chance."""
    return ActionOutcome(False, float(req.params.get("wait_s", 60)), {"waiting_for": "self-recovery"})


ACTIONS: dict[str, Callable[[Cluster, ActionRequest], ActionOutcome]] = {
    "bump_memory_limit": bump_memory_limit,
    "scale_deployment": scale_deployment,
    "wait_and_verify": wait_and_verify,
}
