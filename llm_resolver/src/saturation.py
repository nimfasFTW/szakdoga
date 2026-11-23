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
            f'{{namespace="{namespace}"}}[{self._window_s}s]))'
            f' / '
            f'sum by (namespace, pod) (kube_pod_container_resource_limits'
            f'{{namespace="{namespace}", resource="cpu"}}))'
        )
        try:
            r = self._http.get(self._url, params={"query": query}, timeout=self._timeout_s)
            r.raise_for_status()
            result = r.json()["data"]["result"]
            return float(result[0]["value"][1]) if result else None
        except Exception as e:   # must never abort a verification
            logger.warning("saturation query failed: %r", e)
            return None
