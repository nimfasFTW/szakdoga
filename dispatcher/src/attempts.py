from __future__ import annotations

import threading
import time
from typing import Callable, Optional

class AttemptStore:
    def __init__(self, ttl_s: Optional[float] = 600.0, clock: Callable[[], float] = time.monotonic) -> None:
        self._entries: dict[str, tuple[int, float]] = {}   # key -> (count, last_update)
        self._ttl = ttl_s
        self._clock = clock
        self._lock = threading.Lock()

    def _live(self, key: str) -> int:
        entry = self._entries.get(key)
        if entry is None:
            return 0
        count, updated = entry
        if self._ttl is not None and self._clock() - updated > self._ttl:
            del self._entries[key]
            return 0
        return count

    def get(self, workload_key: str) -> int:
        with self._lock:
            return self._live(workload_key)

    def increment(self, workload_key: str) -> int:
        with self._lock:
            count = self._live(workload_key) + 1
            self._entries[workload_key] = (count, self._clock())
            return count

    def reset(self, workload_key: str) -> None:
        with self._lock:
            self._entries.pop(workload_key, None)

    def reset_all(self) -> None:
        with self._lock:
            self._entries.clear()
