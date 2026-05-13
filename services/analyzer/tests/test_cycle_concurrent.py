"""Concurrency tests for cycle() in services/analyzer/worker.py."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

from services.analyzer.worker import cycle


def _targets(n: int) -> list[tuple[str, str, None]]:
    return [(f"player{i}", f"game{i}", None) for i in range(n)]


def _make_pg_pool(conns: list[MagicMock]) -> MagicMock:
    pool = MagicMock()
    pool.getconn.side_effect = conns
    return pool


def _make_sr_pool(conns: list[MagicMock]) -> MagicMock:
    pool = MagicMock()
    pool.get_connection.side_effect = conns
    return pool


@patch("services.analyzer.worker.fetch_eligible_players")
def test_cycle_processes_five_players_concurrently(mock_fetch: MagicMock) -> None:
    targets = _targets(5)
    mock_fetch.return_value = targets
    pg_pool = _make_pg_pool([MagicMock() for _ in range(6)])
    sr_pool = _make_sr_pool([MagicMock() for _ in range(5)])

    def process_slowly(*_args: object) -> int:
        time.sleep(0.1)
        return 1

    with patch("services.analyzer.worker.process_player", side_effect=process_slowly):
        start = time.perf_counter()
        result = cycle(pg_pool, sr_pool, batch_users=5)
        elapsed = time.perf_counter() - start

    assert result == 5
    # Sequential would be 5 * 0.1 = 0.5s. Anything under 0.25s proves >2x
    # speedup from parallelization. Loose enough for CI runners.
    assert elapsed < 0.25, f"expected <0.25s parallel, got {elapsed:.3f}s"


@patch("services.analyzer.worker.process_player", return_value=1)
@patch("services.analyzer.worker.fetch_eligible_players")
def test_cycle_uses_one_postgres_connection_per_thread(
    mock_fetch: MagicMock,
    mock_process: MagicMock,
) -> None:
    targets = _targets(5)
    mock_fetch.return_value = targets
    pg_pool = _make_pg_pool([MagicMock() for _ in range(6)])
    sr_pool = _make_sr_pool([MagicMock() for _ in range(5)])

    result = cycle(pg_pool, sr_pool, batch_users=5)

    assert result == 5
    assert mock_process.call_count == 5
    assert pg_pool.getconn.call_count == 6
    assert pg_pool.putconn.call_count == 6


@patch("services.analyzer.worker.process_player", return_value=1)
@patch("services.analyzer.worker.fetch_eligible_players")
def test_cycle_uses_one_starrocks_connection_per_player(
    mock_fetch: MagicMock,
    mock_process: MagicMock,
) -> None:
    targets = _targets(5)
    mock_fetch.return_value = targets
    pg_pool = _make_pg_pool([MagicMock() for _ in range(6)])
    sr_conns = [MagicMock() for _ in range(5)]
    sr_pool = _make_sr_pool(sr_conns)

    result = cycle(pg_pool, sr_pool, batch_users=5)

    assert result == 5
    assert mock_process.call_count == 5
    assert sr_pool.get_connection.call_count == 5
    for sr in sr_conns:
        sr.close.assert_called_once()
