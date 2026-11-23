"""Lightweight smoke tests (no network / no bot token)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.config import AppConfig, ConfigStore, config_hash
from app.core.filters import apply_pre_filters, cooldown_allows
from app.core.metrics import WindowMetrics, compute_window_metrics, robust_z
from app.core.normalize import normalize_user_input, base_from_contract, parse_window_to_seconds
from app.core.signal_manager import format_signal_message
from app.core.signals import FiredSignal, evaluate_signals, s2_regime
from app.delta.confirm import spot_delta_confirm
from app.storage.models import CooldownState, RawTick


def test_normalize():
    cfg = AppConfig().normalization
    assert normalize_user_input("btcusdt", cfg) == "BTC"
    assert normalize_user_input("BTC-USDT", cfg) == "BTC"
    assert normalize_user_input("$BTC", cfg) == "BTC"
    assert normalize_user_input("1000PEPEUSDT", cfg) == "PEPE"
    assert normalize_user_input("Kgen", cfg) == "KGEN"
    assert normalize_user_input("kgen", cfg) == "KGEN"
    assert base_from_contract("1000PEPEUSDT", cfg) == "PEPE"
    assert parse_window_to_seconds("4h") == 14400


def test_robust_z():
    hist = [1, 2, 2, 2, 3, 10]
    z = robust_z(10, hist)
    assert z is not None and z > 0


def test_metrics_and_s1():
    cfg = AppConfig()
    now = datetime.now(timezone.utc)
    ticks = []
    oi = 1_000_000.0
    for i in range(200):
        ticks.append(
            RawTick(
                exchange="binance",
                contract_symbol="BTCUSDT",
                base_symbol="BTC",
                ts_utc=now - timedelta(minutes=200 - i),
                oi_usd=oi * (1 + i * 0.001),
                last_price=60000,
                mark_price=60000,
                funding_rate=0.0001,
                volume_24h=50_000_000,
                long_pct=55,
                short_pct=45,
            )
        )
    before = ticks[0]
    current = ticks[-1]
    # force big growth
    current.oi_usd = before.oi_usd * 1.2
    current.oi_qty = (before.oi_qty or (before.oi_usd / 60000)) * 1.2
    before.oi_qty = before.oi_qty or (before.oi_usd / 60000)
    for t in ticks:
        if t.oi_qty is None and t.oi_usd and t.mark_price:
            t.oi_qty = t.oi_usd / t.mark_price
    m = compute_window_metrics(current, before, ticks, "4h", cfg, "binance")
    assert m.oi_growth_pct is not None and m.oi_growth_pct > 15
    fired = evaluate_signals({"4h": m, "1h": m, "24h": m}, cfg)
    codes = {f.code for f in fired}
    assert "S1" in codes or "S2" in codes


def _metrics(**overrides) -> WindowMetrics:
    base = dict(
        window="15m",
        oi_now=1_000_000,
        oi_before=900_000,
        oi_qty_now=10,
        oi_qty_before=9,
        oi_growth_pct=3.0,
        oi_log_change=None,
        oi_z=None,
        oi_pctl=None,
        price_now=1.1,
        price_before=1.0,
        price_change_pct=1.0,
        price_z=None,
        gap=None,
        ratio=None,
        funding_8h=0.0,
        oi_to_volume=0.1,
        long_pct=50,
        short_pct=50,
        long_pct_before=50,
        short_pct_before=50,
        volume_24h=20_000_000,
        history_days=10,
        new_history=False,
    )
    base.update(overrides)
    return WindowMetrics(**base)


def test_s2_oi_threshold_is_4pct():
    cfg = AppConfig()
    assert s2_regime(_metrics(oi_growth_pct=3.9, price_change_pct=1.0), cfg) is None
    assert s2_regime(_metrics(oi_growth_pct=-4.0, price_change_pct=-1.0), cfg) is not None


def test_s5_fires_on_each_window():
    cfg = AppConfig()
    m5 = _metrics(window="5m", oi_growth_pct=-12, price_change_pct=-1)
    m1 = _metrics(window="1h", oi_growth_pct=-1, price_change_pct=-1)
    fired = evaluate_signals({"5m": m5, "1h": m1}, cfg, had_s1_last_24h=True)
    s5 = [f for f in fired if f.code == "S5"]
    assert [f.window for f in s5] == ["5m"]
    quiet = evaluate_signals({"5m": m5}, cfg, had_s1_last_24h=False)
    assert all(f.code != "S5" for f in quiet)


def test_confirmed_follows_spot_delta_sign():
    buy = spot_delta_confirm({"binance": 1_500_000, "bybit": -200_000}, price_change_pct=1.2)
    assert buy.confirmed is True
    assert buy.leader_exchange == "binance"
    assert buy.total_usd == 1_300_000
    sell = spot_delta_confirm({"binance": 1_500_000, "bybit": -200_000}, price_change_pct=-0.4)
    assert sell.confirmed is False
    text = format_signal_message(
        base="BTC",
        exchange="binance",
        window="15m",
        fired=[FiredSignal("S2", "Режим OI + цена", "15m", {"regime": "OI ↑ + Price ↑ → Новые лонги"})],
        metrics=_metrics(),
        s7_status="local",
        flags={},
        ts=datetime.now(timezone.utc),
        spot_delta=buy,
    )
    assert "confirmed" in text.splitlines()[-1]
    assert "Binance" in text and "всего" in text
    plain = format_signal_message(
        base="BTC",
        exchange="binance",
        window="15m",
        fired=[FiredSignal("S1", "Всплеск OI", "15m")],
        metrics=_metrics(),
        s7_status="confirmed",
        flags={},
        ts=datetime.now(timezone.utc),
        spot_delta=None,
    )
    assert plain.splitlines()[-1] == "new"


def test_confirmed_pushover_cooldown_12h():
    from app.core.confirmed_notify import pushover_due

    now = datetime.now(timezone.utc)
    assert pushover_due(None, now, 12) is True
    assert pushover_due(now - timedelta(hours=11, minutes=59), now, 12) is False
    assert pushover_due(now - timedelta(hours=12), now, 12) is True


def test_filters_f1():
    cfg = AppConfig()
    m = WindowMetrics(
        window="1h",
        oi_now=1000,
        oi_before=900,
        oi_qty_now=1000,
        oi_qty_before=900,
        oi_growth_pct=10,
        oi_log_change=None,
        oi_z=None,
        oi_pctl=None,
        price_now=1.0,
        price_before=1.0,
        price_change_pct=0,
        price_z=None,
        gap=None,
        ratio=None,
        funding_8h=0.01,
        oi_to_volume=0.1,
        long_pct=50,
        short_pct=50,
        long_pct_before=50,
        short_pct_before=50,
        volume_24h=1000,
        history_days=10,
        new_history=False,
    )
    r = apply_pre_filters(
        cfg=cfg,
        base="BTC",
        exchange="binance",
        metrics=m,
        instrument=None,
        tick_age_sec=10,
        interval_sec=60,
    )
    assert r.allow is False and r.drop_reason and r.drop_reason.startswith("F1")


def test_cooldown():
    cfg = AppConfig()
    now = datetime.now(timezone.utc)
    cd = CooldownState(
        user_id=1,
        exchange="binance",
        base_symbol="BTC",
        signal_type="S1",
        last_oi_growth=20,
        last_sent_at=now,
    )
    assert cooldown_allows(cfg=cfg, cooldown=cd, signal_type="S1", oi_growth=21, now=now) is False
    assert cooldown_allows(cfg=cfg, cooldown=cd, signal_type="S1", oi_growth=35, now=now) is True


def test_config_hash(tmp_path):
    p = tmp_path / "config.yaml"
    p.write_text("collector:\n  interval_sec: 60\n", encoding="utf-8")
    store = ConfigStore(p)
    cfg = store.load()
    h = config_hash(cfg)
    assert len(h) == 16
    p.write_text("collector:\n  interval_sec: 120\n", encoding="utf-8")
    changed, msg = store.try_reload()
    assert changed is True


def test_ensure_utc_age():
    from datetime import datetime
    from app.core.timeutils import ensure_utc, utcnow

    naive = datetime(2026, 10, 8, 12, 0, 0)  # as SQLite returns
    aware = ensure_utc(naive)
    assert aware.tzinfo is not None
    age = (utcnow() - aware).total_seconds()
    assert age > 0


if __name__ == "__main__":
    test_normalize()
    test_robust_z()
    test_metrics_and_s1()
    test_filters_f1()
    test_cooldown()
    test_ensure_utc_age()
    print("smoke ok")
