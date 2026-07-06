"""Statistical analysis of soak test time series.

Fits OLS linear regressions to each metric and flags statistically
significant trends that exceed configured safety thresholds.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

THRESHOLD_RSS_KB_PER_S = 2.0
THRESHOLD_THREAD_PER_S = 0.01
THRESHOLD_FD_PER_S = 0.15
THRESHOLD_REDIS_MEM_KB_PER_S = 1.0
THRESHOLD_THROUGHPUT_DECLINE_PCT = 5.0
THRESHOLD_HEARTBEAT_ENTRIES_PER_S = 0.01
THRESHOLD_HEAP_ENTRIES_RATIO = 2.0
THRESHOLD_BUFFER_MEM_BYTES_PER_S = 500.0

_T_CRITICAL = {
    10: 2.228,
    20: 2.086,
    30: 2.042,
    40: 2.021,
    50: 2.009,
    60: 2.000,
    80: 1.990,
    100: 1.984,
    120: 1.980,
    200: 1.972,
    500: 1.965,
}


def _t_critical(n):
    """Look up approximate t_critical for n-2 degrees of freedom at alpha=0.05."""
    df = n - 2
    if df <= 0:
        return float("inf")
    best_key = max(k for k in _T_CRITICAL if k <= max(df, min(_T_CRITICAL)))
    return _T_CRITICAL[best_key]


@dataclass(frozen=True, slots=True)
class TrendResult:
    metric: str
    slope: float
    slope_unit: str
    r_squared: float
    t_statistic: float
    p_significant: bool
    early_mean: float
    late_mean: float
    passed: bool
    failure_reason: str

    def to_dict(self):
        return {
            "metric": self.metric,
            "slope": float(self.slope),
            "slope_unit": self.slope_unit,
            "r_squared": float(self.r_squared),
            "t_statistic": float(self.t_statistic),
            "p_significant": bool(self.p_significant),
            "early_mean": float(self.early_mean),
            "late_mean": float(self.late_mean),
            "passed": bool(self.passed),
            "failure_reason": self.failure_reason,
        }


class SoakAnalyzer:
    def __init__(self, snapshots):
        self._snapshots = snapshots
        self._times = np.array([s.elapsed_s for s in snapshots])
        self._results = None

    def analyze(self):
        if self._results is not None:
            return self._results

        s = self._snapshots
        results = []

        results.append(
            self._linear_trend(
                "vm_rss_kb",
                [snap.vm_rss_kb for snap in s],
                THRESHOLD_RSS_KB_PER_S,
                "KB/s",
            )
        )
        results.append(
            self._linear_trend(
                "thread_count",
                [snap.thread_count for snap in s],
                THRESHOLD_THREAD_PER_S,
                "thr/s",
            )
        )
        results.append(
            self._linear_trend(
                "fd_count",
                [snap.fd_count for snap in s],
                THRESHOLD_FD_PER_S,
                "fd/s",
            )
        )
        results.append(
            self._linear_trend(
                "redis_used_memory",
                [snap.redis_used_memory / 1024 for snap in s],
                THRESHOLD_REDIS_MEM_KB_PER_S,
                "KB/s",
            )
        )
        results.append(
            self._linear_trend(
                "buffer_memory_bytes",
                [snap.buffer_memory_bytes for snap in s],
                THRESHOLD_BUFFER_MEM_BYTES_PER_S,
                "B/s",
            )
        )
        results.append(
            self._throughput_trend(
                [snap.cumulative_dispatches for snap in s],
            )
        )

        entries_result = self._linear_trend(
            "heartbeat_entries",
            [snap.heartbeat_entries for snap in s],
            THRESHOLD_HEARTBEAT_ENTRIES_PER_S,
            "ent/s",
        )
        results.append(entries_result)

        heap_result = self._linear_trend(
            "heartbeat_heap",
            [snap.heartbeat_heap for snap in s],
            float("inf"),
            "ent/s",
        )
        if (
            heap_result.p_significant
            and entries_result.slope > 0
            and heap_result.slope > entries_result.slope * THRESHOLD_HEAP_ENTRIES_RATIO
        ):
            heap_result = TrendResult(
                metric=heap_result.metric,
                slope=heap_result.slope,
                slope_unit=heap_result.slope_unit,
                r_squared=heap_result.r_squared,
                t_statistic=heap_result.t_statistic,
                p_significant=heap_result.p_significant,
                early_mean=heap_result.early_mean,
                late_mean=heap_result.late_mean,
                passed=False,
                failure_reason=(
                    f"heap slope ({heap_result.slope:.4f}/s) exceeds "
                    f"{THRESHOLD_HEAP_ENTRIES_RATIO}x entries slope "
                    f"({entries_result.slope:.4f}/s)"
                ),
            )
        results.append(heap_result)

        self._results = results
        return results

    def summary_dict(self):
        results = self.analyze()
        return {
            "results": [r.to_dict() for r in results],
            "terminal_lines": self.terminal_lines(),
            "time_series": {
                "elapsed_s": [s.elapsed_s for s in self._snapshots],
                "vm_rss_kb": [s.vm_rss_kb for s in self._snapshots],
                "vm_size_kb": [s.vm_size_kb for s in self._snapshots],
                "thread_count": [s.thread_count for s in self._snapshots],
                "thread_names": [list(s.thread_names) for s in self._snapshots],
                "fd_count": [s.fd_count for s in self._snapshots],
                "gc_gen0_collections": [s.gc_gen0_collections for s in self._snapshots],
                "gc_gen1_collections": [s.gc_gen1_collections for s in self._snapshots],
                "gc_gen2_collections": [s.gc_gen2_collections for s in self._snapshots],
                "heartbeat_entries": [s.heartbeat_entries for s in self._snapshots],
                "heartbeat_heap": [s.heartbeat_heap for s in self._snapshots],
                "pool_created": [s.pool_created for s in self._snapshots],
                "pool_available": [s.pool_available for s in self._snapshots],
                "pool_in_use": [s.pool_in_use for s in self._snapshots],
                "redis_used_memory": [s.redis_used_memory for s in self._snapshots],
                "redis_used_memory_rss": [
                    s.redis_used_memory_rss for s in self._snapshots
                ],
                "redis_mem_fragmentation_ratio": [
                    s.redis_mem_fragmentation_ratio for s in self._snapshots
                ],
                "buffer_zcard": [s.buffer_zcard for s in self._snapshots],
                "buffer_memory_bytes": [s.buffer_memory_bytes for s in self._snapshots],
                "concurrency_zcard": [s.concurrency_zcard for s in self._snapshots],
                "concurrency_memory_bytes": [
                    s.concurrency_memory_bytes for s in self._snapshots
                ],
                "dlq_llen": [s.dlq_llen for s in self._snapshots],
                "cumulative_dispatches": [
                    s.cumulative_dispatches for s in self._snapshots
                ],
            },
        }

    def terminal_lines(self):
        results = self.analyze()
        header = (
            f"{'Metric':<30} {'Slope':>14} {'R^2':>8} "
            f"{'Early':>10} {'Late':>10} {'Status'}"
        )
        lines = [header, "-" * len(header)]
        for r in results:
            slope_str = f"{r.slope:+.4f} {r.slope_unit}"
            status = "PASS" if r.passed else "FAIL"
            lines.append(
                f"{r.metric:<30} {slope_str:>14} {r.r_squared:>8.4f} "
                f"{r.early_mean:>10.1f} {r.late_mean:>10.1f} {status}"
            )
        return lines

    def _linear_trend(
        self,
        metric,
        values,
        threshold,
        unit,
        times=None,
    ):
        t = times if times is not None else self._times
        v = np.array(values, dtype=float)
        n = len(t)

        if n < 3:
            return TrendResult(
                metric=metric,
                slope=0.0,
                slope_unit=unit,
                r_squared=0.0,
                t_statistic=0.0,
                p_significant=False,
                early_mean=float(np.mean(v)) if n > 0 else 0.0,
                late_mean=float(np.mean(v)) if n > 0 else 0.0,
                passed=True,
                failure_reason="",
            )

        coeffs = np.polyfit(t, v, 1)
        slope = float(coeffs[0])
        intercept = float(coeffs[1])

        predicted = slope * t + intercept
        ss_res = float(np.sum((v - predicted) ** 2))
        ss_tot = float(np.sum((v - np.mean(v)) ** 2))
        r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

        sxx = float(np.sum((t - np.mean(t)) ** 2))
        if sxx > 0 and n > 2:
            stderr = np.sqrt(ss_res / ((n - 2) * sxx))
            t_stat = slope / stderr if stderr > 0 else 0.0
        else:
            t_stat = 0.0

        t_crit = _t_critical(n)
        significant = abs(t_stat) > t_crit

        split = max(1, n // 5)
        early_mean = float(np.mean(v[:split]))
        late_mean = float(np.mean(v[-split:]))

        passed = True
        reason = ""
        if significant and abs(slope) > threshold:
            passed = False
            reason = (
                f"slope {slope:+.4f} {unit} is significant (t={t_stat:.2f}) "
                f"and exceeds threshold {threshold} {unit}"
            )

        return TrendResult(
            metric=metric,
            slope=slope,
            slope_unit=unit,
            r_squared=r_squared,
            t_statistic=t_stat,
            p_significant=significant,
            early_mean=early_mean,
            late_mean=late_mean,
            passed=passed,
            failure_reason=reason,
        )

    def _throughput_trend(self, cumulative):
        t = self._times
        c = np.array(cumulative, dtype=float)
        n = len(t)

        if n < 4:
            return TrendResult(
                metric="throughput_rate",
                slope=0.0,
                slope_unit="tasks/s^2",
                r_squared=0.0,
                t_statistic=0.0,
                p_significant=False,
                early_mean=0.0,
                late_mean=0.0,
                passed=True,
                failure_reason="",
            )

        dt = np.diff(t)
        dc = np.diff(c)
        rates = dc / np.where(dt > 0, dt, 1.0)
        rate_times = (t[:-1] + t[1:]) / 2.0

        mean_rate = float(np.mean(rates))
        result = self._linear_trend(
            "throughput_rate",
            rates.tolist(),
            0.0,
            "tasks/s^2",
            times=rate_times,
        )

        passed = True
        reason = ""
        if (
            result.p_significant
            and result.slope < 0
            and mean_rate > 0
            and abs(result.slope / mean_rate) * 100 > THRESHOLD_THROUGHPUT_DECLINE_PCT
        ):
            passed = False
            decline_pct = abs(result.slope / mean_rate) * 100
            reason = (
                f"throughput declining at {decline_pct:.1f}% of mean rate "
                f"({mean_rate:.1f} tasks/s)"
            )

        return TrendResult(
            metric="throughput_rate",
            slope=result.slope,
            slope_unit="tasks/s^2",
            r_squared=result.r_squared,
            t_statistic=result.t_statistic,
            p_significant=result.p_significant,
            early_mean=result.early_mean,
            late_mean=result.late_mean,
            passed=passed,
            failure_reason=reason,
        )
