from __future__ import annotations

import threading
from collections import defaultdict


class AttemptStore:
    def __init__(self) -> None:
        self._counts: dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()

    def get(self, workload_key: str) -> int:
        with self._lock:
            return self._counts[workload_key]

    def increment(self, workload_key: str) -> int:
        with self._lock:
            self._counts[workload_key] += 1
            return self._counts[workload_key]

    def reset(self, workload_key: str) -> None:
        with self._lock:
            self._counts.pop(workload_key, None)
