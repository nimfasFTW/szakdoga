from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

# both errors mean the same -> error with the image pull
_WAITING_REASON_ALIASES = {
    "ErrImagePull": "ImagePull",
    "ImagePullBackOff": "ImagePull",
}


def normalize_waiting_reason(reason: Optional[str]) -> Optional[str]:
    if reason is None:
        return None
    return _WAITING_REASON_ALIASES.get(reason, reason)


@dataclass(frozen=True)
class Facts:
    # --- from the alert ---
    alertname: str
    namespace: str
    pod: str
    container: Optional[str] = None
    # --- from the Kubernetes API ---
    pod_found: bool = True
    phase: Optional[str] = None
    workload_kind: Optional[str] = None
    workload_name: Optional[str] = None
    waiting_reason: Optional[str] = None       # already normalized
    termination_reason: Optional[str] = None   # OOMKilled / Error / ...
    exit_code: Optional[int] = None
    restart_count: int = 0                     # logged only, NOT part of the signature
    ready: Optional[bool] = None

    @property
    def workload_key(self) -> str:
        """Identity of the thing we patch/count attempts on (the Deployment, not the pod)."""
        return f"{self.namespace}/{self.workload_kind}/{self.workload_name}"


@dataclass(frozen=True)
class Signature:
    alertname: str
    waiting_reason: Optional[str]
    termination_reason: Optional[str]
    attempt: int 


def build_signature(facts: Facts, attempt: int) -> Signature:
    return Signature(
        alertname=facts.alertname,
        waiting_reason=facts.waiting_reason,
        termination_reason=facts.termination_reason,
        attempt=attempt,
    )
