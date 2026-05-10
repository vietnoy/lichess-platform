import main
from main import coach_rate_check


def test_under_limit_allowed():
    for _ in range(9):
        assert coach_rate_check("session-a") is True


def test_at_limit_throttled():
    for _ in range(10):
        assert coach_rate_check("session-a") is True

    assert coach_rate_check("session-a") is False


def test_distinct_sessions_independent():
    for _ in range(11):
        coach_rate_check("session-a")

    assert coach_rate_check("session-b") is True


def test_window_expires(monkeypatch):
    now = 1000.0

    def fake_monotonic():
        return now

    monkeypatch.setattr(main.time, "monotonic", fake_monotonic)

    for _ in range(10):
        assert coach_rate_check("session-a") is True

    assert coach_rate_check("session-a") is False

    now += 61.0

    assert coach_rate_check("session-a") is True
