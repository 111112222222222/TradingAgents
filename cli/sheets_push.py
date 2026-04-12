"""Push analysis results and sentiment data to Google Sheets."""

import json
import os
from datetime import datetime
from pathlib import Path

SHEET_ID = "1UH_05aaWX3ZYScFxPvJpw3pfkrJwRF7ULBqfyDnRz_A"
SERVICE_ACCOUNT_FILE = Path(__file__).parent.parent / "service-account.json"


def _get_client():
    """Get authenticated gspread client. Returns None if not configured."""
    if not SERVICE_ACCOUNT_FILE.exists():
        return None
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_file(
            str(SERVICE_ACCOUNT_FILE), scopes=scopes
        )
        return gspread.authorize(creds)
    except Exception:
        return None


def push_signal(final_state: dict, ticker: str, model: str = "", provider: str = ""):
    """Push a completed analysis signal to the Signals tab."""
    gc = _get_client()
    if not gc:
        return False

    try:
        sh = gc.open_by_key(SHEET_ID)
        ws = sh.worksheet("Signals")

        # Extract key fields
        invest = final_state.get("investment_debate_state", {})
        risk = final_state.get("risk_debate_state", {})

        # Truncate long fields to 50000 chars (Sheets cell limit)
        def trunc(text, limit=5000):
            if not text:
                return ""
            text = str(text)
            return text[:limit] + "..." if len(text) > limit else text

        row = [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ticker,
            trunc(final_state.get("final_trade_decision", ""), 200),
            model,
            provider,
            trunc(final_state.get("market_report", "")),
            trunc(final_state.get("sentiment_report", "")),
            trunc(final_state.get("news_report", "")),
            trunc(final_state.get("fundamentals_report", "")),
            trunc(invest.get("bull_history", "")),
            trunc(invest.get("bear_history", "")),
            trunc(final_state.get("trader_investment_plan", "")),
            trunc(risk.get("judge_decision", "")),
            trunc(final_state.get("final_trade_decision", "")),
        ]

        ws.append_row(row, value_input_option="USER_ENTERED")
        return True
    except Exception as e:
        print(f"[Sheets] Failed to push signal: {e}")
        return False


def push_sentiment(sentiment_data: dict):
    """Push a sentiment analysis result to the Sentiment tab."""
    gc = _get_client()
    if not gc:
        return False

    try:
        sh = gc.open_by_key(SHEET_ID)
        ws = sh.worksheet("Sentiment")

        scenarios = sentiment_data.get("scenarios", {})
        bull = scenarios.get("bull", {})
        base = scenarios.get("base", {})
        bear = scenarios.get("bear", {})

        # Summarize top factors
        factors = sentiment_data.get("factors", [])
        top_factors = "; ".join(
            f"{f.get('name', '')} ({f.get('impact', '')} w{f.get('weight', '')})"
            for f in factors[:6]
        )

        row = [
            sentiment_data.get("last_updated", datetime.now().isoformat()),
            sentiment_data.get("model", "unknown"),
            "",  # SPX level — filled by caller if available
            "",  # VIX
            "",  # 10Y Yield
            bull.get("probability", ""),
            bull.get("target_range", ""),
            bull.get("thesis", ""),
            base.get("probability", ""),
            base.get("target_range", ""),
            base.get("thesis", ""),
            bear.get("probability", ""),
            bear.get("target_range", ""),
            bear.get("thesis", ""),
            sentiment_data.get("change_summary", ""),
            top_factors,
        ]

        ws.append_row(row, value_input_option="USER_ENTERED")
        return True
    except Exception as e:
        print(f"[Sheets] Failed to push sentiment: {e}")
        return False


def push_stagflation(signal_values: dict, metadata: dict = None, changes: list = None):
    """Push a stagflation snapshot to the Stagflation tab."""
    gc = _get_client()
    if not gc:
        return False

    try:
        sh = gc.open_by_key(SHEET_ID)

        # Create tab if it doesn't exist
        try:
            ws = sh.worksheet("Stagflation")
        except Exception:
            ws = sh.add_worksheet(title="Stagflation", rows=1000, cols=20)
            headers = [
                "Timestamp", "VIX", "S&P Drawdown %", "Put/Call",
                "HY Spread (bps)", "Lending Std %",
                "CPI YoY %", "Core PCE %", "Unemployment %",
                "PMI", "GDP QoQ %", "Fed Funds %",
                "S&P Level", "Changes",
            ]
            ws.append_row(headers, value_input_option="USER_ENTERED")

        meta = metadata or {}
        change_str = "; ".join(changes[:6]) if changes else ""

        row = [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            signal_values.get("vix", ""),
            signal_values.get("sp500_drawdown", ""),
            signal_values.get("put_call", ""),
            signal_values.get("hy_spread", ""),
            signal_values.get("lending_standards", ""),
            signal_values.get("cpi", ""),
            signal_values.get("pce", ""),
            signal_values.get("unemployment", ""),
            signal_values.get("pmi", ""),
            signal_values.get("gdp", ""),
            signal_values.get("fed_funds", ""),
            meta.get("_sp500_level", ""),
            change_str,
        ]

        ws.append_row(row, value_input_option="USER_ENTERED")
        return True
    except Exception as e:
        print(f"[Sheets] Failed to push stagflation: {e}")
        return False
