"""At most one in-flight incident per workload.

Why it is needed: one crash-looping pod raises two alerts (CrashLooping + NotReady), Alertmanager
re-sends firing alerts every repeat_interval, and our own fix creates a new pod that raises alerts
of its own. Without this, the same playbook would run several times for a single incident.

The lease is a safety net: if a worker dies without releasing, the lock expires on its own.
"""
from __future__ import annotations

import threading
import time
from typing import Callable


class IncidentGuard:
    def __init__(self, lease_s: float = 300.0, clock: Callable[[], float] = time.monotonic) -> None:
        self._held: dict[str, float] = {}   # workload key -> acquired at
        self._lease = lease_s
        self._clock = clock
        self._lock = threading.Lock()

    def try_acquire(self, workload_key: str) -> bool:
        with self._lock:
            acquired = self._held.get(workload_key)
            if acquired is not None and self._clock() - acquired < self._lease:
                return False
            self._held[workload_key] = self._clock()
            return True

    def release(self, workload_key: str) -> None:
        with self._lock:
            self._held.pop(workload_key, None)

    def reset_all(self) -> None:
        with self._lock:
            self._held.clear()

    def held(self) -> list[str]:
        with self._lock:
            return sorted(self._held)
