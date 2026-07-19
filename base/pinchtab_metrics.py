"""Lightweight in-process metrics for the pinchtab HTTP client.

Tracks HTTP-call count, latency, and outcome per endpoint so we can see if
an extraction job is being wasteful (e.g. 9 RTTs when 2 would suffice).
Used by pinchtab_client + daemons for diagnostics and overload-detection.

Cheap to write/read (atomic counters under threading.Lock).  No Prometheus,
no DB — just plain in-memory state per process — so no impact on the daemon
hot path beyond a single GIL-acquire for the snapshot reads.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger("pinchtab_metrics")


@dataclass
class _BucketStats:
    calls: int = 0
    errors: int = 0
    recoveries: int = 0
    total_seconds: float = 0.0

    @property
    def avg_ms(self) -> float:
        return (self.total_seconds / self.calls * 1000) if self.calls else 0.0


class PinchtabMetrics:
    """Shared singleton; safe to call from any coroutine."""

    def __init__(self):
        self._lock = threading.Lock()
        self._buckets: dict[str, _BucketStats] = defaultdict(_BucketStats)
        self._started_at = time.monotonic()

    def record(self, endpoint: str, seconds: float, error: bool = False, recovered: bool = False):
        with self._lock:
            b = self._buckets[endpoint]
            b.calls += 1
            b.total_seconds += seconds
            if error:
                b.errors += 1
            if recovered:
                b.recoveries += 1

    def snapshot(self) -> dict:
        """Thread-local-safe snapshot for printing.  Returns dict ready for JSON."""
        with self._lock:
            return {
                "uptime_seconds": round(time.monotonic() - self._started_at, 1),
                "endpoints": {
                    ep: {
                        "calls": b.calls,
                        "errors": b.errors,
                        "recoveries": b.recoveries,
                        "avg_ms": round(b.avg_ms, 2),
                    }
                    for ep, b in self._buckets.items()
                },
            }

    def reset(self):
        with self._lock:
            self._buckets.clear()
            self._started_at = time.monotonic()


# Process-global singleton
_INSTANCE: Optional[PinchtabMetrics] = None


def get() -> PinchtabMetrics:
    global _INSTANCE
    if _INSTANCE is None:
        _INSTANCE = PinchtabMetrics()
    return _INSTANCE


def reset() -> None:
    global _INSTANCE
    if _INSTANCE is not None:
        _INSTANCE.reset()
