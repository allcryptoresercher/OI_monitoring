from app.chart.align import align_market_delta


def test_market_delta_sums_minutes_inside_candle():
    opens = [0, 15 * 60_000]
    rows = [
        ("binance", 60_000, 10.0),
        ("binance", 2 * 60_000, -4.0),
        ("bybit", 60_000, 3.0),
        ("binance", 16 * 60_000, 7.0),
        ("binance", 40 * 60_000, 100.0),
    ]
    out = align_market_delta(opens, rows, 15 * 60_000)
    assert out["binance"] == [6.0, 7.0]
    assert out["bybit"][0] == 3.0
    assert out["bybit"][1] is None
