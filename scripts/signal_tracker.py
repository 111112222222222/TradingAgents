#!/usr/bin/env python3
"""Track performance of past BUY/SELL signals against actual price moves.

Scans eval_results/ for all past signal runs, extracts the signal from
final_trade_decision, fetches historical prices via yfinance, and writes
a performance summary to .dashboard/signal-performance.json.

Run manually or on cron:
    python scripts/signal_tracker.py
"""

import json
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import yfinance as yf

EVAL_DIR = PROJECT_ROOT / "eval_results"
DASHBOARD_DIR = PROJECT_ROOT / ".dashboard"
OUTPUT_FILE = DASHBOARD_DIR / "signal-performance.json"

SIGNAL_PATTERN = re.compile(
    r"\b(BUY|ACCUMULATE|OVERWEIGHT|HOLD|REDUCE|UNDERWEIGHT|SELL)\b", re.IGNORECASE
)

# Normalize alternate labels to canonical form
SIGNAL_NORMALIZE = {
    "ACCUMULATE": "OVERWEIGHT",
    "REDUCE": "UNDERWEIGHT",
}


def extract_signal(text: str | None) -> str | None:
    """Extract first signal keyword from text, normalized to canonical form."""
    if not text:
        return None
    m = SIGNAL_PATTERN.search(text)
    if not m:
        return None
    raw = m.group(1).upper()
    return SIGNAL_NORMALIZE.get(raw, raw)


def unwrap_state_json(data: dict) -> dict:
    """If JSON has a single date key at top level, unwrap it."""
    keys = list(data.keys())
    if len(keys) == 1 and re.match(r"^\d{4}-\d{2}-\d{2}$", keys[0]):
        return data[keys[0]]
    return data


def scan_eval_results() -> list[dict]:
    """Scan eval_results/ for all past signal runs."""
    runs = []
    if not EVAL_DIR.is_dir():
        return runs

    for ticker_dir in sorted(EVAL_DIR.iterdir()):
        if not ticker_dir.is_dir():
            continue
        ticker = ticker_dir.name
        logs_dir = ticker_dir / "TradingAgentsStrategy_logs"
        if not logs_dir.is_dir():
            continue

        for f in sorted(logs_dir.iterdir()):
            m = re.match(r"^full_states_log_(.+)\.json$", f.name)
            if not m:
                continue
            date_str = m.group(1)
            try:
                with open(f) as fh:
                    raw = json.load(fh)
                state = unwrap_state_json(raw)
                decision = state.get("final_trade_decision", "")
                signal = extract_signal(decision)
                if signal:
                    runs.append({
                        "ticker": ticker,
                        "signal_date": date_str,
                        "signal": signal,
                    })
            except (json.JSONDecodeError, OSError):
                continue
    return runs


def fetch_prices(runs: list[dict]) -> dict:
    """Batch-fetch historical close prices for all tickers using yfinance.download().

    Returns a dict: { ticker: { "YYYY-MM-DD": close_price, ... } }
    """
    if not runs:
        return {}

    tickers = sorted(set(r["ticker"] for r in runs))
    # Find the earliest signal date to set the download start
    dates = [r["signal_date"] for r in runs]
    earliest = min(dates)
    # Start 1 day before earliest signal to ensure we capture it
    start_dt = datetime.strptime(earliest, "%Y-%m-%d") - timedelta(days=3)
    start_str = start_dt.strftime("%Y-%m-%d")

    print(f"Downloading prices for {len(tickers)} tickers from {start_str}...")

    # yfinance download for multiple tickers
    df = yf.download(tickers, start=start_str, auto_adjust=True, progress=False)

    prices: dict[str, dict[str, float]] = {}

    if df.empty:
        return prices

    # Handle single vs multi-ticker DataFrame structure
    if len(tickers) == 1:
        t = tickers[0]
        prices[t] = {}
        close_series = df["Close"]
        for dt_idx, val in close_series.items():
            if val is not None and val == val:  # not NaN
                date_key = dt_idx.strftime("%Y-%m-%d")
                prices[t][date_key] = round(float(val), 2)
    else:
        close_df = df["Close"]
        for t in tickers:
            prices[t] = {}
            if t not in close_df.columns:
                continue
            col = close_df[t]
            for dt_idx, val in col.items():
                if val is not None and val == val:  # not NaN
                    date_key = dt_idx.strftime("%Y-%m-%d")
                    prices[t][date_key] = round(float(val), 2)

    return prices


def find_price_on_or_after(price_data: dict[str, float], target_date: str, max_days: int = 5) -> float | None:
    """Find the closing price on target_date or the nearest trading day after."""
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    for offset in range(max_days + 1):
        key = (dt + timedelta(days=offset)).strftime("%Y-%m-%d")
        if key in price_data:
            return price_data[key]
    return None


def find_price_at_offset(price_data: dict[str, float], target_date: str, days: int) -> float | None:
    """Find the closing price approximately `days` trading days after target_date."""
    dt = datetime.strptime(target_date, "%Y-%m-%d") + timedelta(days=days)
    # Look within a window of +/- 3 days to handle weekends/holidays
    for offset in range(-3, 4):
        key = (dt + timedelta(days=offset)).strftime("%Y-%m-%d")
        if key in price_data:
            return price_data[key]
    return None


def calc_return(price_then: float | None, price_now: float | None) -> float | None:
    """Calculate percentage return."""
    if price_then is None or price_now is None or price_then == 0:
        return None
    return round((price_now - price_then) / price_then * 100, 2)


def get_latest_price(price_data: dict[str, float]) -> tuple[float | None, str | None]:
    """Get the most recent price from the price data."""
    if not price_data:
        return None, None
    latest_date = max(price_data.keys())
    return price_data[latest_date], latest_date


def build_performance(runs: list[dict], prices: dict) -> list[dict]:
    """Enrich each run with price performance data."""
    today = datetime.now()
    results = []

    for run in runs:
        ticker = run["ticker"]
        signal_date = run["signal_date"]
        signal = run["signal"]
        ticker_prices = prices.get(ticker, {})

        price_at_signal = find_price_on_or_after(ticker_prices, signal_date)
        current_price, _ = get_latest_price(ticker_prices)

        signal_dt = datetime.strptime(signal_date, "%Y-%m-%d")
        days_since = (today - signal_dt).days

        # Calculate returns at various intervals
        return_1d = None
        return_1w = None
        return_1m = None

        if price_at_signal is not None:
            if days_since >= 1:
                p = find_price_at_offset(ticker_prices, signal_date, 1)
                return_1d = calc_return(price_at_signal, p)
            if days_since >= 7:
                p = find_price_at_offset(ticker_prices, signal_date, 7)
                return_1w = calc_return(price_at_signal, p)
            if days_since >= 30:
                p = find_price_at_offset(ticker_prices, signal_date, 30)
                return_1m = calc_return(price_at_signal, p)

        results.append({
            "ticker": ticker,
            "signal_date": signal_date,
            "signal": signal,
            "price_at_signal": price_at_signal,
            "price_current": current_price,
            "return_1d": return_1d,
            "return_1w": return_1w,
            "return_1m": return_1m,
            "days_since": days_since,
        })

    return results


def build_summary(signals: list[dict]) -> dict:
    """Build aggregate summary stats."""
    total = len(signals)
    buy_signals = [s for s in signals if s["signal"] == "BUY"]
    sell_signals = [s for s in signals if s["signal"] == "SELL"]
    overweight_signals = [s for s in signals if s["signal"] == "OVERWEIGHT"]
    underweight_signals = [s for s in signals if s["signal"] == "UNDERWEIGHT"]
    hold_signals = [s for s in signals if s["signal"] == "HOLD"]

    # Use the best available return for each signal (prefer 1m > 1w > 1d)
    def best_return(s: dict) -> float | None:
        for key in ("return_1m", "return_1w", "return_1d"):
            if s.get(key) is not None:
                return s[key]
        # Fall back to current price vs signal price
        if s.get("price_at_signal") and s.get("price_current"):
            return calc_return(s["price_at_signal"], s["price_current"])
        return None

    buy_returns = [best_return(s) for s in buy_signals if best_return(s) is not None]
    sell_returns = [best_return(s) for s in sell_signals if best_return(s) is not None]

    buy_avg = round(sum(buy_returns) / len(buy_returns), 2) if buy_returns else None
    sell_avg = round(sum(sell_returns) / len(sell_returns), 2) if sell_returns else None

    # Win rate: for BUY, positive return = win; for SELL, negative return = win
    buy_wins = sum(1 for r in buy_returns if r > 0) if buy_returns else 0
    win_rate_buy = round(buy_wins / len(buy_returns), 2) if buy_returns else None

    sell_wins = sum(1 for r in sell_returns if r < 0) if sell_returns else 0
    win_rate_sell = round(sell_wins / len(sell_returns), 2) if sell_returns else None

    return {
        "total": total,
        "buy_count": len(buy_signals),
        "sell_count": len(sell_signals),
        "overweight_count": len(overweight_signals),
        "underweight_count": len(underweight_signals),
        "hold_count": len(hold_signals),
        "buy_avg_return": buy_avg,
        "sell_avg_return": sell_avg,
        "win_rate_buy": win_rate_buy,
        "win_rate_sell": win_rate_sell,
    }


def main():
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)

    print("Scanning eval_results/ for signal runs...")
    runs = scan_eval_results()
    print(f"Found {len(runs)} signals")

    if not runs:
        output = {
            "last_updated": datetime.now().isoformat(),
            "signals": [],
            "summary": build_summary([]),
        }
        OUTPUT_FILE.write_text(json.dumps(output, indent=2))
        print(f"Wrote empty results to {OUTPUT_FILE}")
        return

    prices = fetch_prices(runs)
    signals = build_performance(runs, prices)

    # Sort by date descending
    signals.sort(key=lambda s: s["signal_date"], reverse=True)

    output = {
        "last_updated": datetime.now().isoformat(),
        "signals": signals,
        "summary": build_summary(signals),
    }

    OUTPUT_FILE.write_text(json.dumps(output, indent=2))
    print(f"Wrote {len(signals)} signals to {OUTPUT_FILE}")
    summary = output["summary"]
    print(f"Summary: {summary['total']} total, {summary['buy_count']} BUY, {summary['sell_count']} SELL")
    if summary["win_rate_buy"] is not None:
        print(f"BUY win rate: {summary['win_rate_buy']:.0%}, avg return: {summary['buy_avg_return']}%")
    if summary["win_rate_sell"] is not None:
        print(f"SELL win rate: {summary['win_rate_sell']:.0%}, avg return: {summary['sell_avg_return']}%")


if __name__ == "__main__":
    main()
