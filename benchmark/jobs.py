"""Finite-job ledger. Workers execute at least once; writeback is idempotent."""
from collections import deque
import time


class JobLedger:
    def __init__(self, players, base, size, lease_seconds, max_attempts):
        self.jobs = [(lo, min(lo + size, base + players)) for lo in range(base, base + players, size)]
        self.pending = deque(range(len(self.jobs)))
        self.inflight = {}
        self.attempts = {}
        self.completed = set()
        self.failed = {}
        self.lease_seconds = lease_seconds
        self.max_attempts = max_attempts
        self.totals = {"patrons": 0, "daily_rows": 0, "profiles_written": 0}

    def expire(self, now=None):
        now = time.monotonic() if now is None else now
        for job, state in list(self.inflight.items()):
            if state["deadline"] <= now:
                self._retry(job, "lease expired")

    def _retry(self, job, reason):
        self.inflight.pop(job, None)
        if self.attempts[job] >= self.max_attempts:
            self.failed[job] = reason
        else:
            self.pending.append(job)

    def claim(self, owner, now=None):
        now = time.monotonic() if now is None else now
        self.expire(now)
        if not self.pending:
            return None
        job = self.pending.popleft()
        attempt = self.attempts.get(job, 0) + 1
        self.attempts[job] = attempt
        self.inflight[job] = {"owner": owner, "attempt": attempt, "deadline": now + self.lease_seconds}
        return {"job": job, "attempt": attempt, "lo": self.jobs[job][0], "hi": self.jobs[job][1]}

    def heartbeat(self, owner, job, attempt):
        state = self.inflight.get(job)
        if state and (state["owner"], state["attempt"]) == (owner, attempt):
            state["deadline"] = time.monotonic() + self.lease_seconds

    def finish(self, owner, job, attempt, counts=None, error=None):
        state = self.inflight.get(job)
        if not state or (state["owner"], state["attempt"]) != (owner, attempt):
            return False  # stale/duplicate acknowledgments cannot inflate logical totals
        if error:
            self._retry(job, error)
        else:
            self.inflight.pop(job)
            self.completed.add(job)
            for key in self.totals:
                self.totals[key] += (counts or {}).get(key, 0)
        return True

    @property
    def done(self):
        return not self.pending and not self.inflight

    def summary(self):
        return {"total_jobs": len(self.jobs), "completed_jobs": len(self.completed),
                "failed_jobs": len(self.failed), "unfinished_jobs": len(self.jobs) - len(self.completed) - len(self.failed),
                "attempts": sum(self.attempts.values()), "totals": self.totals,
                "failures": self.failed}
