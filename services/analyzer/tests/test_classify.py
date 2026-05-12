"""Tests for classify_move in services/analyzer/worker.py."""

import pytest
from services.analyzer.worker import classify_move


@pytest.mark.parametrize(
    "cp_before, cp_after, mover, expected_swing, expected_cls",
    [
        # white blunder: drop = 50 - (-200) = 250
        (50, -200, "white", -250, "blunder"),
        # black blunder: drop = 200 - (-50) = 250, swing = -50 - 200 = -250
        (-50, 200, "black", -250, "blunder"),
        # white mistake at boundary: drop = 100
        (100, 0, "white", -100, "mistake"),
        # just below mistake: drop = 99 -> inaccuracy
        (99, 0, "white", -99, "inaccuracy"),
        # white inaccuracy at boundary: drop = 50
        (50, 0, "white", -50, "inaccuracy"),
        # just below inaccuracy: drop = 49 -> good
        (49, 0, "white", -49, "good"),
        # white improves: drop = -50 -> good, swing = +50
        (0, 50, "white", 50, "good"),
        # black improves: drop = -50 - 0 = -50 -> good, swing = 0 - (-50) = +50
        (0, -50, "black", 50, "good"),
    ],
)
def test_classify_move_parametrized(
    cp_before: int,
    cp_after: int,
    mover: str,
    expected_swing: int,
    expected_cls: str,
) -> None:
    swing, cls = classify_move(cp_before, cp_after, mover)
    assert swing == expected_swing
    assert cls == expected_cls


def test_none_cp_before() -> None:
    assert classify_move(None, 100, "white") == (None, None)


def test_none_cp_after() -> None:
    assert classify_move(100, None, "white") == (None, None)


def test_both_none() -> None:
    assert classify_move(None, None, "white") == (None, None)
