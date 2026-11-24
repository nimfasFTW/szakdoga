"""CPU saturation of a workload, read from Prometheus.

Why this exists: 'rolled out + ready + no restarts' (verify.py) is also true for a pod that is
overloaded but alive. For a load problem (UC-9) that definition would call 'did nothing' a success.
So a workload only counts as healthy if none of its pods uses more than `max_cpu_util` of its CPU limit.

Same ratio as the ChaosPodCPUSaturated alert, but evaluated by us per verification poll,
for the pods of ONE Deployment. We look at the workload's resource usage, not at whether the alert
has cleared (the alert lags behind and its pod names change with every rollout).

Best effort by design: if Prometheus cannot be reached, or the pods have no CPU limit / no samples yet,
the answer is None ('unknown') and the CPU criterion is skipped, so it cannot break the other use cases.
"""
from __future__ import annotations

import logging
import math
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
        self.problem: Optional[str] = None     # why the last answer was None ('no_data', 'query_failed: ...')
        self._logged: set = set()

    def _unknown(self, problem: str, query: str) -> None:
        self.problem = problem
        if problem not in self._logged:        # log each kind once: the query can be pasted into the Prometheus UI
            self._logged.add(problem)
            logger.warning("CPU saturation unknown (%s). URL=%s query=%s", problem, self._url, query)

    def max_cpu_utilisation(self, namespace: str, pods: Sequence[str]) -> Optional[float]:
        """Highest (CPU usage / CPU limit) over the given pods, or None if unknown."""
        self.problem = None
        if not pods or not _SAFE.match(namespace):
            self._unknown("unsafe_or_empty_names", "")
            return None
        wanted = set(pods)
        query = (
            f'sum by (namespace, pod) (rate(container_cpu_usage_seconds_total'
            f'{{namespace="{namespace}"}}[{self._window_s}s]))'
            f' / on (namespace, pod) '
            f'sum by (namespace, pod) (kube_pod_container_resource_limits'
            f'{{namespace="{namespace}", resource="cpu"}})'
        )
        try:
            r = self._http.get(self._url, params={"query": query}, timeout=self._timeout_s)
            r.raise_for_status()
            series = r.json()["data"]["result"]
            values = [float(x["value"][1]) for x in series if x.get("metric", {}).get("pod") in wanted]
            values = [v for v in values if math.isfinite(v)]
            if not values:
                self._unknown("no_data", query)
                return None
            return max(values)
        except Exception as e:   # must never abort a verification
            self._unknown(f"query_failed: {type(e).__name__}", query)
            logger.warning("saturation query error: %r", e)
            return None
