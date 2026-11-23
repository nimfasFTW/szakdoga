"""CPU saturation of a workload, read from Prometheus.

Why this exists: 'rolled out + ready + no restarts' (verify.py) is also true for a pod that is
overloaded but alive. For a load problem (UC-9) that definition would call 'did nothing' a success.
So a workload only counts as healthy if none of its pods uses more than `max_cpu_util` of its CPU limit.

Same expression as the ChaosPodCPUSaturated alert (pod level), but evaluated by us per verification poll,
for the pods of ONE Deployment. We look at the workload's resource usage, not at whether the alert
has cleared (the alert lags behind and its pod names change with every rollout).

Best effort by design: if Prometheus cannot be reached, or the pods have no CPU limit / no samples yet,
the answer is None ('unknown') and the CPU criterion is skipped, so it cannot break the other use cases.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Optional, Sequence

import requests

logger = logging.getLogger("llm_resolver.saturation")

_SAFE = re.compile(r"^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$")   # Kubernetes object names; keeps the PromQL injection-free


class PrometheusSaturation:
    def __init__(self, base_url: str, window_s: int = 30, timeout_s: float = 3.0, http: Any = requests):
        self._url = base_url.rstrip("/") + "/api/v1/query"
        self._window_s = window_s
        self._timeout_s = timeout_s
        self._http = http

    def max_cpu_utilisation(self, namespace: str, pods: Sequence[str]) -> Optional[float]:
        """Highest (CPU usage / CPU limit) over the given pods, or None if unknown."""
        if not pods or not _SAFE.match(namespace) or not all(_SAFE.match(p) for p in pods):
            return None
        pod_re = "|".join(pods)
        # pod level (container=""): the only level cAdvisor exposes on Minikube with the Docker driver
        query = (
            f'max(sum by (namespace, pod) (rate(container_cpu_usage_seconds_total'
            f'{{namespace="{namespace}", pod=~"{pod_re}", container=""}}[{self._window_s}s]))'
            f' / on (namespace, pod) '
            f'sum by (namespace, pod) (kube_pod_container_resource_limits'
            f'{{namespace="{namespace}", pod=~"{pod_re}", resource="cpu"}}))'
        )
        try:
            r = self._http.get(self._url, params={"query": query}, timeout=self._timeout_s)
            r.raise_for_status()
            result = r.json()["data"]["result"]
            return float(result[0]["value"][1]) if result else None
        except Exception as e:   # must never abort a verification
            logger.warning("saturation query failed: %r", e)
            return None
