import chess


def _fake_eval(fen: str, depth: int | None = None) -> dict:
    board = chess.Board(fen)
    best_move = "e7e5" if board.turn == chess.BLACK else "g1f3"
    return {"cp": 50, "best_move": best_move, "mate": None}


def test_whatif_returns_two_lines(client, monkeypatch):
    monkeypatch.setattr("main.eval_fen", _fake_eval)

    response = client.post(
        "/api/whatif",
        json={
            "base_fen": chess.STARTING_FEN,
            "actual_uci": "e2e4",
            "alt_uci": "d2d4",
            "plies": 3,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["actual"]) == 3
    assert len(payload["alt"]) == 3


def test_whatif_invalid_plies(client, monkeypatch):
    monkeypatch.setattr("main.eval_fen", _fake_eval)

    response = client.post(
        "/api/whatif",
        json={
            "base_fen": chess.STARTING_FEN,
            "actual_uci": "e2e4",
            "alt_uci": "d2d4",
            "plies": 20,
        },
    )

    assert response.status_code == 400


def test_whatif_invalid_uci_breaks_early(client, monkeypatch):
    monkeypatch.setattr("main.eval_fen", _fake_eval)

    response = client.post(
        "/api/whatif",
        json={
            "base_fen": chess.STARTING_FEN,
            "actual_uci": "e2e4",
            "alt_uci": "z9z9",
            "plies": 3,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["alt"]) == 0
