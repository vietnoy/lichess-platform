from main import Metrics
from stockfish import clear_eval_cache


def test_render_empty():
    clear_eval_cache()
    rendered = Metrics.render()

    assert "# HELP http_requests_total Total HTTP requests by route and status" in rendered
    assert "# HELP http_request_latency_ms_p99 99th percentile request latency in ms (rolling 500 samples)" in rendered
    assert "# HELP coach_throttled_total Number of /api/coach requests rate-limited (HTTP 429)" in rendered
    assert "# HELP stockfish_eval_cache_hits_total Stockfish eval cache hits" in rendered
    assert "stockfish_eval_cache_entries 0" in rendered


def test_record_increments_counter():
    Metrics.record("/api/x", 200, 5.0)
    Metrics.record("/api/x", 200, 5.0)

    rendered = Metrics.render()

    assert 'http_requests_total{route="/api/x",status="200"} 2' in rendered


def test_p99_p50_calculated():
    for latency_ms in range(1, 101):
        Metrics.record("/api/x", 200, float(latency_ms))

    rendered = Metrics.render().splitlines()
    p99_line = next(line for line in rendered if line.startswith('http_request_latency_ms_p99{route="/api/x"} '))
    p50_line = next(line for line in rendered if line.startswith('http_request_latency_ms_p50{route="/api/x"} '))
    p99 = float(p99_line.rsplit(" ", 1)[1])
    p50 = float(p50_line.rsplit(" ", 1)[1])

    assert abs(p99 - 99.0) <= 2.0
    assert abs(p50 - 50.0) <= 2.0


def test_coach_throttled_counter():
    Metrics.coach_throttled()

    rendered = Metrics.render()

    assert "coach_throttled_total 1" in rendered
