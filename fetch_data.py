# fetch_data.py
import os, io, time, requests, pandas as pd
from datetime import datetime
import yfinance as yf

BASE = "data"
RAW  = f"{BASE}/01_raw_daily_utc"
JST  = f"{BASE}/02_jst_daily"
OUT  = f"{BASE}/99_outputs"
for d in (RAW, JST, OUT):
    os.makedirs(d, exist_ok=True)

START = "2005-01-01"
END   = datetime.utcnow().strftime("%Y-%m-%d")

def try_stooq_csv(symbols):
    url_tpl = "https://stooq.com/q/d/l/?s={sym}&i=d"
    for sym in symbols:
        url = url_tpl.format(sym=sym)
        try:
            r = requests.get(url, timeout=20)
            if r.status_code == 200 and r.text.lower().startswith("date,"):
                df = pd.read_csv(io.StringIO(r.text))
                if not df.empty:
                    df.rename(columns={"Date":"date"}, inplace=True)
                    df["datetime_utc"] = pd.to_datetime(df["date"]).dt.tz_localize("UTC")
                    for c in ["Open","High","Low","Close"]:
                        if c not in df.columns: df[c] = pd.NA
                    if "Adj Close" not in df.columns: df["Adj Close"] = df["Close"]
                    if "Volume" not in df.columns: df["Volume"] = pd.NA
                    return df[["datetime_utc","Open","High","Low","Close","Adj Close","Volume"]]
        except Exception:
            pass
        time.sleep(0.7)
    return pd.DataFrame()

def try_yf(ticker):
    try:
        df = yf.download(ticker, start=START, end=END, interval="1d", auto_adjust=False, progress=False)
        if df.empty: return pd.DataFrame()
        df = df.reset_index().rename(columns={"Date":"datetime_utc"})
        df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
        need = ["Open","High","Low","Close","Adj Close","Volume"]
        for c in need:
            if c not in df.columns: df[c] = pd.NA
        return df[["datetime_utc","Open","High","Low","Close","Adj Close","Volume"]]
    except Exception:
        return pd.DataFrame()

TARGETS = {
    "Nikkei225":       {"stooq": ['^nkx','^nikkei','^n225','n225','nikkei'], "yahoo": []},
    "NASDAQ100":       {"stooq": ['^ndx'],        "yahoo": ['^NDX']},
    "DowJones":        {"stooq": ['^dji'],        "yahoo": ['^DJI']},
    "USDJPY":          {"stooq": ['usdjpy','jpyusd'], "yahoo": ['JPY=X']},
    "Nikkei_CFD":      {"stooq": ['jpn225','jp225'],  "yahoo": ['JP225USD=X']},
    "Nikkei_Leverage": {"stooq": [],                  "yahoo": ['1570.T','1321.T']},
}

def save_pair(df, name):
    df.to_csv(f"{RAW}/{name}.csv", index=False)
    jst = df.copy()
    jst["datetime_jst"] = jst["datetime_utc"].dt.tz_convert("Asia/Tokyo")
    jst.to_csv(f"{JST}/{name}.csv", index=False)
    return jst

frames = []
for name, src in TARGETS.items():
    df = pd.DataFrame()
    if src["stooq"]:
        df = try_stooq_csv(src["stooq"])
    if df.empty and src["yahoo"]:
        for t in src["yahoo"]:
            df = try_yf(t)
            if not df.empty: break
    if df.empty:
        print(f"⚠️ {name}: 取得失敗（後日再試行）")
        continue
    frames.append(save_pair(df, name).assign(symbol=name))

if frames:
    merged = pd.concat(frames, ignore_index=True).sort_values(["symbol","datetime_jst"])
    merged.to_csv(f"{OUT}/unified_daily_jst.csv", index=False)
    print("✅ 統合CSV:", f"{OUT}/unified_daily_jst.csv", "shape=", merged.shape)
else:
    print("⚠️ 今回は全て失敗。時間を置いて再実行してください。")
