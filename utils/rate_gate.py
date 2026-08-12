"""utils/rate_gate.py — Cross-thread/loop mutex + cooldown gate.

Wraps the recurring "two globals + a lock + time.monotonic()" pattern that
appears in the DDGS-backed scripts and the pinchtab browser client.

Standardizing on `time.monotonic()` (was a mix of `time.time()`,
`asyncio.get_running_loop().time()`, and `time.monotonic()`) avoids wall-clock
drift on long-running daemons. Standardizing on `threading.Lock` for the
shared float avoids asyncio/threading mismatches: the read+write is a
single quick critical section with no `await` inside it, so threading.Lock
is correct under both event-loop and worker-thread contexts.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Callable

log = logging.getLogger("rate_gate")

LockFactory = Callable[[], threading.Lock]


def _thread_lock_factory() -> threading.Lock:
    return threading.Lock()


class RateGate:
    """Mutex-guarded read/write of a `cooldown_until: float` epoch.

    Usage:
        gate = RateGate()
        if gate.in_cooldown():
            return
        ... # do work, possibly increment streak
        gate.record_streak(new_streak, threshold=3, backoff_s=300)

    The lock is created on first use so the constructor itself is cheap.
    """

    def __init__(self, lock_factory: LockFactory = _thread_lock_factory):
        self._lock_factory = lock_factory
        self._lock: threading.Lock | None = None
        self._cooldown_until: float = 0.0

    def _get_lock(self) -> threading.Lock:
        if self._lock is None:
            self._lock = self._lock_factory()
        return self._lock

    def in_cooldown(self) -> bool:
        """True iff the cooldown window is still in the future."""
        with self._get_lock():
            return time.monotonic() < self._cooldown_until

    def record_streak(self, streak: int, threshold: int, backoff_s: float) -> None:
        """Trip cooldown if streak crosses threshold and cooldown is not active.

        Designed to be safe to call on every HTTP response without leaking
        time math into the caller.
        """
        with self._get_lock():
            now = time.monotonic()
            if streak >= threshold and self._cooldown_until <= now:
                self._cooldown_until = now + backoff_s
                log.warning(
                    "RateGate cooldown started: streak=%d threshold=%d backoff=%.0fs",
                    streak, threshold, backoff_s,
                )
