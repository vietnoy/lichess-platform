import requests

from stockfish import DEFAULT_DEPTH, eval_fen


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
