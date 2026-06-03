import requests

import stockfish
from stockfish import clear_eval_cache, eval_fen


def setup_function():
    clear_eval_cache()


def test_eval_returns_none_on_network_error(monkeypatch):
    def raise_error(*args, **kwargs):
        raise requests.RequestException("boom")

    monkeypatch.setattr("stockfish.requests.get", raise_error)

    assert eval_fen("some-fen") is None


def test_eval_passes_depth_param(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"cp": 12, "best_move": "e2e4", "mate": None}

    def fake_get(url, params, timeout):
        captured["url"] = url
        captured["params"] = params
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr("stockfish.requests.get", fake_get)

    result = eval_fen("some-fen", depth=17)

    assert result == {"cp": 12, "best_move": "e2e4", "mate": None}
    assert captured["params"]["depth"] == 17
    assert captured["params"]["fen"] == "some-fen"
    assert captured["timeout"] == 15


def test_eval_uses_success_cache(monkeypatch):
    calls = 0

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"cp": 12, "best_move": "e2e4", "mate": None}

    def fake_get(url, params, timeout):
        nonlocal calls
        calls += 1
        return FakeResponse()

    monkeypatch.setattr("stockfish.requests.get", fake_get)

    assert eval_fen("same-fen", depth=12) == {"cp": 12, "best_move": "e2e4", "mate": None}
    assert eval_fen("same-fen", depth=12) == {"cp": 12, "best_move": "e2e4", "mate": None}
    assert calls == 1


def test_eval_cache_is_keyed_by_normalized_depth(monkeypatch):
    depths = []

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"cp": 12, "best_move": "e2e4", "mate": None}

    def fake_get(url, params, timeout):
        depths.append(params["depth"])
        return FakeResponse()

    monkeypatch.setattr("stockfish.requests.get", fake_get)
    monkeypatch.setattr(stockfish, "MAX_DEPTH", 18)

    eval_fen("same-fen", depth=99)
    eval_fen("same-fen", depth=18)

    assert depths == [18]


def test_eval_cache_can_be_disabled(monkeypatch):
    calls = 0

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"cp": 12, "best_move": "e2e4", "mate": None}

    def fake_get(url, params, timeout):
        nonlocal calls
        calls += 1
        return FakeResponse()

    monkeypatch.setattr("stockfish.requests.get", fake_get)
    monkeypatch.setattr(stockfish, "EVAL_CACHE_MAX_ENTRIES", 0)

    eval_fen("same-fen", depth=12)
    eval_fen("same-fen", depth=12)

    assert calls == 2
