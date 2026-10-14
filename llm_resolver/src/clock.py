"""Time source that tests can replace, so a 60 s verification window runs instantly."""
import time


class Clock:
    def now(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)
