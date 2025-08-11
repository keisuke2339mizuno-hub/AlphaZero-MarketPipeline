# fetch_data.py  —  実データ取得→CSV保存（Stooq優先 / yfinanceフォールバック）
import os, io, time, requests, pandas as pd
from datetime import datetime, timezone

BASE = "data"
RAW  = os.path.join(BASE, "01_raw_daily_utc")
JST  = os.path.join(BASE, "02_jst_daily")
OUT  = os.path.join(BASE, "99_outputs")
for d in (RAW, JST, OUT):
    os.makedirs(d, exist_ok=True)

def try_stooq(symbols):
    """候補記号を順に試し、取れたCSVをDataFrameで返す"""
    url = "https://stooq.com/q/d/l/?s={}&i=d"
    for s in symbols:
        try:
            r = requests.get(url.format(s), timeout=15)
            if r.status_code == 200 and r.text.lower().startswith("date,"):
                df = pd.read_csv(io.StringIO(r.text))
                if not df.empty:
                    df.rename(columns={"Date":"date"}, inplace=True)
                    df["datetime_utc"] = pd.to_datetime(df["date"]).dt.tz_localize("UTC")
                    for c in ["Open","High","Low","Close"]:
                        if c not in df: df[c] = pd.NA
                    if "Adj Close" not in df: df["Adj Close"] = df["Close"]
                    if "Volume" not in df: df["Volume"] = pd.NA
                    out = df[["datetime_utc","Open","High","Low","Close","Adj Close","Volume"]].copy()
                    print(f"  ✅ Stooq {s}: rows={len(out)}")
                    return out
            else:
                print(f"  … Stooq {s}: status={r.status_code}")
        except Exception as e:
            print(f"  … Stooq {s}: err={e}")
        time.sleep(0.6)
    return pd.DataFrame()

def try_yf(ticker):
    import yfinance as yf
    try:
        df = yf.download(ticker, interval="1d", auto_adjust=False, progress=False)
        if df.empty:
            return pd.DataFrame()
        df = df.reset_index().rename(columns={"Date":"datetime_utc"})
        df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
        need = ["Open","High","Low","Close","Adj Close","Volume"]
        for c in need:
            if c not in df: df[c] = pd.NA
        out = df[["datetime_utc","Open","High","Low","Close","Adj Close","Volume"]].copy()
        print(f"  ✅ Yahoo {ticker}: rows={len(out)}")
        return out
    except Exception as e:
        print(f"  … Yahoo {ticker}: err={e}")
        return pd.DataFrame()

TARGETS = {
    # まず日本系はStooq優先（Yahooが弾かれることがあるため）
    "Nikkei225":       {"stooq": ['^nkx','^nikkei','^n225','n225'], "yahoo": ['^N225']},
    "NASDAQ100":       {"stooq": ['^ndx'],                          "yahoo": ['^NDX']},
    "DowJones":        {"stooq": ['^dji'],                          "yahoo": ['^DJI']},
    "USDJPY":          {"stooq": ['usdjpy','jpyusd'],               "yahoo": ['JPY=X']},
    # CFD/先物/1570 は環境によりブロックされやすいので別途追加予定
}

frames = []
print("⏳ Fetch start")
for name, src in TARGETS.items():
    df = pd.DataFrame()
    if src["stooq"]:
        df = try_stooq(src["stooq"])
    if df.empty and src["yahoo"]:
        for t in src["yahoo"]:
            df = try_yf(t)
            if not df.empty:
                break
    if df.empty:
        print(f"  ⚠️ {name}: 取得失敗（後で再試行）")
        continue

    # 保存（UTC/JST）と統合用
    df.to_csv(os.path.join(RAW, f"{name}.csv"), index=False)
    jst = df.copy()
    jst["datetime_jst"] = jst["datetime_utc"].dt.tz_convert("Asia/Tokyo")
    jst.to_csv(os.path.join(JST, f"{name}.csv"), index=False)
    jst["symbol"] = name
    frames.append(jst)

if frames:
    merged = pd.concat(frames, ignore_index=True).sort_values(["symbol","datetime_jst"])
    out_csv = os.path.join(OUT, "unified_daily_jst.csv")
    merged.to_csv(out_csv, index=False)
    print(f"🎉 DONE: {out_csv}  shape={merged.shape}")
else:
    print("⚠️ すべて失敗。時間を置いて再実行してください。")
