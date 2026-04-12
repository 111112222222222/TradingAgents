#!/usr/bin/env python3
"""Fetch stagflation monitor signals from Yahoo Finance and FRED.

Writes results to .dashboard/stagflation-data.json for the dashboard to consume.
Run on cron every 4 hours.
"""

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import yfinance as yf
from fredapi import Fred
from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

DASHBOARD_DIR = PROJECT_ROOT / ".dashboard"
OUTPUT_FILE = DASHBOARD_DIR / "stagflation-data.json"

# Alert thresholds — mirrors dashboard page definitions
THRESHOLDS = {
    # Credit
    'hy_spread': {'warn': 500, 'trigger': 650, 'name': 'HY Spread (bps)'},
    'lending_standards': {'warn': 30, 'trigger': 50, 'name': 'Lending Standards'},
    # Equity
    'sp500_drawdown': {'warn': 20, 'trigger': 30, 'name': 'S&P 500 Drawdown %'},
    'vix': {'warn': 28, 'trigger': 35, 'name': 'VIX'},
    'put_call': {'warn': 1.1, 'trigger': 1.3, 'name': 'Put/Call Ratio'},
    # Macro (inverted — below threshold is the signal)
    'pmi': {'warn': 48, 'trigger': 45, 'name': 'ISM PMI', 'inverted': True},
    'gdp': {'warn': 0.5, 'trigger': 0, 'name': 'GDP QoQ %', 'inverted': True},
}

def fetch_yahoo_signals() -> dict:
    """Fetch market signals from Yahoo Finance."""
    signals = {}
    now = datetime.now()

    # VIX
    try:
        vix = yf.Ticker("^VIX")
        hist = vix.history(period="1d")
        if not hist.empty:
            signals["vix"] = round(float(hist["Close"].iloc[-1]), 2)
    except Exception as e:
        print(f"[stagflation] VIX fetch failed: {e}")

    # S&P 500 drawdown from 52-week high
    try:
        sp = yf.Ticker("^GSPC")
        hist = sp.history(period="1y")
        if not hist.empty:
            high_52w = float(hist["High"].max())
            current = float(hist["Close"].iloc[-1])
            drawdown = round(((high_52w - current) / high_52w) * 100, 2)
            signals["sp500_drawdown"] = max(drawdown, 0)
            signals["_sp500_level"] = round(current, 2)
            signals["_sp500_52w_high"] = round(high_52w, 2)
    except Exception as e:
        print(f"[stagflation] S&P 500 fetch failed: {e}")

    # Put/Call ratio — use CBOE equity put/call via ticker
    try:
        # Use total market put/call from index options
        # Yahoo doesn't have a direct put/call ticker, so we approximate
        # from VIX term structure (VIX vs VIX3M)
        vix_val = signals.get("vix")
        vix3m = yf.Ticker("^VIX3M")
        hist3m = vix3m.history(period="1d")
        if not hist3m.empty and vix_val:
            vix3m_val = float(hist3m["Close"].iloc[-1])
            # Inverted term structure (VIX > VIX3M) implies fear/high put buying
            # Map ratio: VIX/VIX3M of 1.0 = neutral, >1.1 = elevated fear
            ratio = round(vix_val / vix3m_val, 2) if vix3m_val > 0 else None
            if ratio:
                signals["put_call"] = ratio
                signals["_vix3m"] = round(vix3m_val, 2)
    except Exception as e:
        print(f"[stagflation] VIX3M fetch failed: {e}")

    # AAII sentiment — not directly on Yahoo, skip (needs web scrape)

    return signals


def fetch_fred_signals() -> dict:
    """Fetch macro indicators and credit signals from FRED."""
    signals = {}
    try:
        fred = Fred(api_key=os.getenv("FRED_API_KEY"))
    except Exception as e:
        print(f"[stagflation] FRED client init failed: {e}")
        return signals

    # Map of signal_id -> (FRED series, transform)
    fred_series = {
        # Macro indicators
        "cpi": ("CPIAUCSL", "yoy"),           # CPI YoY
        "pce": ("PCEPILFE", "yoy"),           # Core PCE YoY
        "unemployment": ("UNRATE", "level"),   # Unemployment rate
        "pmi": ("MANEMP", None),               # ISM doesn't have a direct FRED series
        "gdp": ("A191RL1Q225SBEA", "level"),   # Real GDP QoQ annualized
        "fed_funds": ("FEDFUNDS", "level"),    # Fed Funds Rate

        # Credit signals
        "hy_spread": ("BAMLH0A0HYM2", "level"),  # ICE BofA HY OAS
    }

    for signal_id, (series_id, transform) in fred_series.items():
        try:
            data = fred.get_series(series_id)
            if data is not None and len(data) > 0:
                if transform == "yoy":
                    # Calculate year-over-year percent change
                    if len(data) >= 13:
                        current = float(data.iloc[-1])
                        year_ago = float(data.iloc[-13])  # ~12 months ago
                        yoy_pct = round(((current - year_ago) / year_ago) * 100, 2)
                        signals[signal_id] = yoy_pct
                elif transform == "level":
                    signals[signal_id] = round(float(data.iloc[-1]), 2)
        except Exception as e:
            print(f"[stagflation] FRED {signal_id} ({series_id}) failed: {e}")

    # ISM Manufacturing PMI — use a different series
    try:
        # NAPM is the old code; use ISM Manufacturing PMI composite
        data = fred.get_series("NAPM")
        if data is not None and len(data) > 0:
            signals["pmi"] = round(float(data.iloc[-1]), 2)
    except Exception:
        try:
            # Fallback: Manufacturing employment as proxy
            data = fred.get_series("MANEMP")
            if data is not None and len(data) > 0:
                # Just note it as available, not a direct PMI
                pass
        except Exception:
            pass

    # Senior Loan Officer Survey — lending standards tightening
    try:
        data = fred.get_series("DRTSCILM")  # Net % tightening standards for C&I loans
        if data is not None and len(data) > 0:
            signals["lending_standards"] = round(float(data.iloc[-1]), 2)
    except Exception as e:
        print(f"[stagflation] Lending standards fetch failed: {e}")

    # HY spread is in bps (FRED reports in percentage points, multiply by 100)
    if "hy_spread" in signals:
        signals["hy_spread"] = round(signals["hy_spread"] * 100, 0)

    return signals


def check_alerts(signal_values: dict, prev_values: dict) -> list:
    """Check if any signals crossed threshold levels."""
    alerts = []
    for sig_id, thresholds in THRESHOLDS.items():
        current = signal_values.get(sig_id)
        prev = prev_values.get(sig_id)
        if current is None:
            continue
        current = float(current)
        prev = float(prev) if prev is not None else None

        inverted = thresholds.get('inverted', False)
        warn = thresholds['warn']
        trigger = thresholds['trigger']
        name = thresholds['name']

        def get_level(val, _inverted=inverted, _warn=warn, _trigger=trigger):
            if _inverted:
                if val <= _trigger: return 'trigger'
                if val <= _warn: return 'warn'
                return 'none'
            else:
                if val >= _trigger: return 'trigger'
                if val >= _warn: return 'warn'
                return 'none'

        current_level = get_level(current)
        prev_level = get_level(prev) if prev is not None else 'none'

        if current_level != prev_level and current_level != 'none':
            emoji = '\U0001f534' if current_level == 'trigger' else '\U0001f7e1'
            label = 'DEPLOY TRIGGER' if current_level == 'trigger' else 'WATCH'
            alerts.append(f"{emoji} *{name}*: {prev or '?'} \u2192 {current} [{label}] (W:{warn} T:{trigger})")

    return alerts


def send_slack_alert(alerts: list):
    """Send alerts via Slack webhook."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url or not alerts:
        return

    text = "*Stagflation Monitor Alert*\n" + "\n".join(alerts)
    try:
        import urllib.request
        data = json.dumps({"text": text}).encode()
        req = urllib.request.Request(webhook_url, data=data, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)
        print(f"[stagflation] Sent {len(alerts)} alert(s) to Slack")
    except Exception as e:
        print(f"[stagflation] Slack alert failed: {e}")


def fetch_all():
    """Fetch all signals and save to dashboard JSON."""
    print(f"[stagflation] Fetching signals at {datetime.now().isoformat()}")

    yahoo = fetch_yahoo_signals()
    fred_data = fetch_fred_signals()

    yahoo_status = "ok" if yahoo else "down"
    fred_status = "ok" if fred_data else "down"

    # Merge (yahoo takes precedence for overlapping keys)
    all_signals = {**fred_data, **yahoo}

    # Separate metadata (keys starting with _) from signal values
    metadata = {k: v for k, v in all_signals.items() if k.startswith("_")}
    signal_values = {k: v for k, v in all_signals.items() if not k.startswith("_")}

    # Load existing data to preserve history
    existing = {}
    if OUTPUT_FILE.exists():
        try:
            existing = json.loads(OUTPUT_FILE.read_text())
        except Exception:
            pass

    existing_signals = existing.get("signals", {})
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    # Update signals with history
    for signal_id, value in signal_values.items():
        prev = existing_signals.get(signal_id, {"value": "", "history": []})
        history = prev.get("history", [])
        str_value = str(value)
        # Only add to history if value changed
        if str_value != str(prev.get("value", "")):
            history.append({"date": now_str, "value": str_value})
            if len(history) > 24:
                history = history[-24:]
        existing_signals[signal_id] = {"value": str_value, "history": history}

    output = {
        "signals": existing_signals,
        "metadata": metadata,
        "last_fetched": datetime.now().isoformat(),
        "sources": {
            "yahoo": list(yahoo.keys()),
            "fred": list(fred_data.keys()),
        },
        "source_status": {
            "yahoo": yahoo_status,
            "fred": fred_status,
        },
    }

    DASHBOARD_DIR.mkdir(exist_ok=True)
    tmp = OUTPUT_FILE.with_suffix('.tmp')
    tmp.write_text(json.dumps(output, indent=2))
    tmp.rename(OUTPUT_FILE)

    # Save daily snapshot (latest fetch per day wins, same as sentiment)
    history_dir = DASHBOARD_DIR / "history"
    history_dir.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    snapshot_file = history_dir / f"stagflation-{date_str}.json"

    # Build change log vs previous snapshot
    changes = []
    prev_snapshot = None
    # Find most recent previous snapshot
    existing_snapshots = sorted(history_dir.glob("stagflation-*.json"), reverse=True)
    for sf in existing_snapshots:
        if sf.name != f"stagflation-{date_str}.json":
            try:
                prev_snapshot = json.loads(sf.read_text())
            except Exception:
                pass
            break

    if prev_snapshot:
        prev_values = prev_snapshot.get("signal_values", {})
        for k, v in signal_values.items():
            prev_v = prev_values.get(k)
            if prev_v is not None and prev_v != v:
                try:
                    diff = round(float(v) - float(prev_v), 2)
                    direction = "+" if diff > 0 else ""
                    changes.append(f"{k}: {prev_v} -> {v} ({direction}{diff})")
                except (ValueError, TypeError):
                    changes.append(f"{k}: {prev_v} -> {v}")

    # Check for threshold crossings and send Slack alerts
    prev_sig_values = prev_snapshot.get("signal_values", {}) if prev_snapshot else {}
    alerts = check_alerts(signal_values, prev_sig_values)
    if alerts:
        print(f"[stagflation] {len(alerts)} threshold alert(s) detected")
        send_slack_alert(alerts)

    snapshot = {
        "snapshot_date": date_str,
        "fetched_at": datetime.now().isoformat(),
        "signal_values": signal_values,
        "metadata": metadata,
        "changes": changes,
    }
    tmp_snap = snapshot_file.with_suffix('.tmp')
    tmp_snap.write_text(json.dumps(snapshot, indent=2))
    tmp_snap.rename(snapshot_file)

    print(f"[stagflation] Saved {len(signal_values)} signals to {OUTPUT_FILE}")
    print(f"[stagflation] Snapshot: {snapshot_file.name}")
    if changes:
        print(f"[stagflation] Changes from previous:")
        for c in changes:
            print(f"  {c}")
    else:
        print(f"[stagflation] No changes from previous snapshot")
    for k, v in sorted(signal_values.items()):
        print(f"  {k}: {v}")

    # Push to Google Sheets (best-effort)
    try:
        from cli.sheets_push import push_stagflation
        if push_stagflation(signal_values, metadata, changes):
            print("[stagflation] Pushed to Google Sheets")
    except Exception as e:
        print(f"[stagflation] Sheets push failed: {e}")

    return output


if __name__ == "__main__":
    fetch_all()
