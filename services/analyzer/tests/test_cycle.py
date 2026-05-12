"""Tests for cycle() in services/analyzer/worker.py."""

from unittest.mock import MagicMock, call, patch

import pytest

from services.analyzer.worker import cycle


def _make_pg() -> MagicMock:
    return MagicMock()


def _make_sr() -> MagicMock:
    return MagicMock()


def _targets(n: int) -> list[tuple]:
    return [(f"player{i}", f"game{i}", None) for i in range(n)]


# ---------------------------------------------------------------------------
# 1. Happy path: 3 players, all succeed
# ---------------------------------------------------------------------------


@patch("services.analyzer.worker.process_player")
@patch("services.analyzer.worker.fetch_eligible_players")
def test_cycle_happy_path_three_players(mock_fetch, mock_process):
    targets = _targets(3)
    mock_fetch.return_value = targets
    mock_process.side_effect = [5, 7, 2]

    pg = _make_pg()
    sr = _make_sr()

    result = cycle(pg, sr)

    assert result == 3
    # SELECT transaction released before per-player work begins
    pg.commit.assert_called_once()
    assert mock_process.call_count == 3
    mock_process.assert_any_call(pg, sr, "player0", "game0", None)
    mock_process.assert_any_call(pg, sr, "player1", "game1", None)
    mock_process.assert_any_call(pg, sr, "player2", "game2", None)


# ---------------------------------------------------------------------------
# 2. One player fails; others still processed
# ---------------------------------------------------------------------------


@patch("services.analyzer.worker.process_player")
@patch("services.analyzer.worker.fetch_eligible_players")
def test_cycle_one_player_fails_others_continue(mock_fetch, mock_process):
    targets = _targets(3)
    mock_fetch.return_value = targets
    mock_process.side_effect = [5, RuntimeError("boom"), 2]

    pg = _make_pg()
    sr = _make_sr()

    result = cycle(pg, sr)

    assert result == 3
    assert mock_process.call_count == 3
    # Exactly one rollback — only the failing player should trigger it.
    assert pg.rollback.call_count == 1


# ---------------------------------------------------------------------------
# 3. Empty target list
# ---------------------------------------------------------------------------


@patch("services.analyzer.worker.process_player")
@patch("services.analyzer.worker.fetch_eligible_players")
def test_cycle_empty_target_list(mock_fetch, mock_process):
    mock_fetch.return_value = []

    pg = _make_pg()
    sr = _make_sr()

    result = cycle(pg, sr)

    assert result == 0
    mock_process.assert_not_called()
    # commit still called to release the SELECT transaction
    pg.commit.assert_called_once()


# ---------------------------------------------------------------------------
# 4. batch_users is forwarded to fetch_eligible_players
# ---------------------------------------------------------------------------


@patch("services.analyzer.worker.process_player")
@patch("services.analyzer.worker.fetch_eligible_players")
def test_cycle_passes_batch_users(mock_fetch, mock_process):
    mock_fetch.return_value = []

    pg = _make_pg()
    sr = _make_sr()

    cycle(pg, sr, batch_users=10)

    mock_fetch.assert_called_once_with(pg, 10)


# ---------------------------------------------------------------------------
# 5. fetch_eligible_players failure propagates (main() handles reconnect)
# ---------------------------------------------------------------------------


@patch("services.analyzer.worker.process_player")
@patch("services.analyzer.worker.fetch_eligible_players")
def test_cycle_fetch_failure_propagates(mock_fetch, mock_process):
    mock_fetch.side_effect = RuntimeError("starrocks down")

    pg = _make_pg()
    sr = _make_sr()

    with pytest.raises(RuntimeError):
        cycle(pg, sr)

    mock_process.assert_not_called()
