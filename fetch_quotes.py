"""Fetch latest index quotes -> data/raw/<date>/quotes_<ts>.json and data/latest.json
Run: pip install yfinance pandas && python ingest/fetch_quotes.py
"""
import json, datetime as dt
from pathlib import Path
from zoneinfo import ZoneInfo
import pandas as pd
import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]
dim = pd.read_csv(ROOT / "seeds" / "dim_country_index.csv")
MAX_ABS_MOVE = 25.0  # % ; larger moves flagged as suspect (bad tick / ticker change)


def market_status(row, now_utc):
    local = now_utc.astimezone(ZoneInfo(row.timezone))
    if local.weekday() >= 5:
        return "closed"
    o = dt.time.fromisoformat(row.open_local)
    c = dt.time.fromisoformat(row.close_local)
    return "open" if o <= local.time() <= c else "closed"


def fetch_one(ticker):
    """Source adapter: swap or chain providers here (Stooq, MOEX ISS...)."""
    h = yf.Ticker(ticker).history(period="5d", interval="1d")
    h = h.dropna(subset=["Close"])
    if len(h) < 2:
        return None
    last, prev = float(h["Close"].iloc[-1]), float(h["Close"].iloc[-2])
    return {"last": last, "prev_close": prev, "as_of": h.index[-1].isoformat()}


def main():
    now = dt.datetime.now(dt.timezone.utc)
    rows = []
    for r in dim.itertuples():
        try:
            q = fetch_one(r.yahoo_ticker)
        except Exception as e:
            q, err = None, str(e)
        else:
            err = None if q else "no_data"
        rec = {"iso3": r.iso3, "country": r.country, "continent": r.continent,
               "index_name": r.index_name, "ticker": r.yahoo_ticker,
               "market_status": market_status(r, now), "fetched_at": now.isoformat(),
               "last": None, "prev_close": None, "as_of": None,
               "pct_change": None, "quality": "missing", "error": err}
        if q:
            pct = (q["last"] - q["prev_close"]) / q["prev_close"] * 100
            rec.update(last=q["last"], prev_close=q["prev_close"], as_of=q["as_of"],
                       pct_change=round(pct, 2),
                       quality="suspect" if abs(pct) > MAX_ABS_MOVE else "ok")
        rows.append(rec)

    raw_dir = ROOT / "data" / "raw" / now.strftime("%Y-%m-%d")
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / f"quotes_{now.strftime('%H%M%S')}.json").write_text(json.dumps(rows, indent=2))
    (ROOT / "data").mkdir(exist_ok=True)
    (ROOT / "data" / "latest.json").write_text(json.dumps(rows, indent=2))
    ok = sum(r["quality"] == "ok" for r in rows)
    print(f"{ok}/{len(rows)} markets ok")


if __name__ == "__main__":
    main()
