import pytest

import benchmarks.run_benchmarks as benchmark_module


def test_verify_enqueued_jobs_rejects_incorrect_queued_count(monkeypatch):
    monkeypatch.setattr(
        benchmark_module,
        "_run_query",
        lambda query, params=None: [(5, 5, 4)],
    )

    with pytest.raises(
        RuntimeError,
        match="expected 5 queued jobs, got 4",
    ):
        benchmark_module._verify_enqueued_jobs(expected_count=5)
