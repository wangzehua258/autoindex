#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
市场数据抓取脚本
从 Yahoo Finance 和 FRED 抓取市场数据，保存到 CSV 并推送到 Google Sheets
"""

import os
import sys
import time
import json
import math
from datetime import datetime, timezone
import pandas as pd
import numpy as np
import yfinance as yf
from pandas_datareader import data as pdr

# Google Sheets 相关（可选）
try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False

# ---------- 配置 ----------
OUTPUT_DIR = "data"
HISTORY_CSV = os.path.join(OUTPUT_DIR, "history.csv")
LATEST_CSV = os.path.join(OUTPUT_DIR, "latest.csv")

# Google Sheets 配置（从环境变量读取）
GOOGLE_SHEETS_CREDENTIALS_JSON = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON")
GOOGLE_SHEETS_SPREADSHEET_ID = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
GOOGLE_SHEETS_HISTORY_SHEET = "History"
GOOGLE_SHEETS_LATEST_SHEET = "Latest"

# 指数 & 商品 & 美元指数（Yahoo）
YF_TICKERS = {
    # 股指
    "S&P 500": "^GSPC",
    "Dow Jones": "^DJI",
    "Nasdaq Composite": "^IXIC",
    "Euro Stoxx 50": "^STOXX50E",
    "Stoxx Europe 600": "^STOXX",
    # 美元指数（使用备用符号，因为 ^DXY 经常不可用）
    "US Dollar Index (DXY)": "DX-Y.NYB",
    # 主要货币对
    "EUR/USD": "EURUSD=X",
    "USD/JPY": "JPY=X",
    "GBP/USD": "GBPUSD=X",
    "USD/CHF": "CHF=X",
    "AUD/USD": "AUDUSD=X",
    "USD/CAD": "CAD=X",
    "NZD/USD": "NZDUSD=X",
    # 大宗
    "WTI Crude (CL)": "CL=F",
    "Brent Crude (BZ)": "BZ=F",
    "Gold (GC)": "GC=F",
    "Copper (HG)": "HG=F",
}

# FRED 国债收益率（单位：%）
FRED_SERIES = {
    "US 10Y Yield (DGS10)": "DGS10",
    "US 2Y  Yield (DGS2)": "DGS2",
}

# ---------- 函数 ----------


def ensure_dir(path: str):
    """确保目录存在"""
    if not os.path.exists(path):
        os.makedirs(path)


def fetch_yfinance(tickers: dict) -> pd.DataFrame:
    """从 yfinance 拉行情，输出列：name, symbol, price, prev_close, change_pct, source"""
    rows = []
    for name, symbol in tickers.items():
        try:
            t = yf.Ticker(symbol)
            info = t.history(period="5d", interval="1d")  # 拉5天做容错
            
            if info.empty:
                raise RuntimeError(f"No data for {symbol}")
            
            # 取最后一条为最新
            latest = info.tail(1)
            price = float(latest["Close"].iloc[0])
            
            # 前收
            if len(info) >= 2:
                prev_close = float(info["Close"].iloc[-2])
            else:
                prev_close = float(latest["Close"].iloc[0])
            
            change_pct = (price / prev_close - 1.0) * 100 if prev_close else None
            
            rows.append({
                "category": "INDEX/FX/COMMOD",
                "name": name,
                "symbol": symbol,
                "value": price,
                "prev_close": prev_close,
                "change_pct": change_pct,
                "unit": "",  # 价格单位多样，这里留空/由名称识别
                "source": "yfinance",
            })
        except Exception as e:
            print(f"[WARN] Failed to fetch {symbol}: {e}", file=sys.stderr)
            rows.append({
                "category": "INDEX/FX/COMMOD",
                "name": name,
                "symbol": symbol,
                "value": None,
                "prev_close": None,
                "change_pct": None,
                "unit": "",
                "source": f"yfinance_error:{e}",
            })
            continue
        
        # 礼貌性限速，避免频繁请求
        time.sleep(0.2)
    
    return pd.DataFrame(rows)


def fetch_fred(series: dict) -> pd.DataFrame:
    """从 FRED 拉 10Y/2Y 收益率，输出列：name, value(%), unit='%'"""
    rows = []
    for name, code in series.items():
        try:
            df = pdr.DataReader(code, "fred")
            if df.empty:
                raise RuntimeError(f"No FRED data for {code}")
            
            value = float(df.iloc[-1, 0])  # 百分比
            
            rows.append({
                "category": "BOND_YIELD",
                "name": name,
                "symbol": code,
                "value": value,
                "prev_close": None,
                "change_pct": None,
                "unit": "%",
                "source": "fred",
            })
        except Exception as e:
            print(f"[WARN] Failed to fetch FRED {code}: {e}", file=sys.stderr)
            rows.append({
                "category": "BOND_YIELD",
                "name": name,
                "symbol": code,
                "value": None,
                "prev_close": None,
                "change_pct": None,
                "unit": "%",
                "source": f"fred_error:{e}",
            })
            continue
        
        time.sleep(0.1)
    
    # 计算 10Y-2Y 利差（基点）
    try:
        d = {r["symbol"]: r for r in rows}
        if "DGS10" in d and "DGS2" in d and d["DGS10"]["value"] is not None and d["DGS2"]["value"] is not None:
            spread_bp = (d["DGS10"]["value"] - d["DGS2"]["value"]) * 100  # 百分点 -> 基点
            rows.append({
                "category": "BOND_YIELD",
                "name": "US 10Y-2Y Spread",
                "symbol": "SPREAD_10Y_2Y",
                "value": round(spread_bp, 1),
                "prev_close": None,
                "change_pct": None,
                "unit": "bp",
                "source": "calc",
            })
    except Exception:
        pass
    
    return pd.DataFrame(rows)


def clean_dataframe_for_export(df: pd.DataFrame) -> pd.DataFrame:
    """清理 DataFrame 中的 NaN、inf 等值，使其可以安全地导出到 CSV 和 Google Sheets"""
    df_clean = df.copy()
    
    # 对于数值列，将 NaN 和 inf 替换为 None（在 CSV 中会变成空字符串）
    numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        df_clean[col] = df_clean[col].replace([np.inf, -np.inf], None)
        df_clean[col] = df_clean[col].where(pd.notnull(df_clean[col]), None)
    
    return df_clean


def assemble_and_save(df_list):
    """合并数据并保存到 CSV"""
    ensure_dir(OUTPUT_DIR)
    ts = datetime.now(timezone.utc)
    ts_iso = ts.isoformat()
    
    # 过滤掉空 DataFrame
    non_empty_dfs = [df for df in df_list if not df.empty]
    if not non_empty_dfs:
        print("[WARN] No data to save")
        return pd.DataFrame()
    
    df = pd.concat(non_empty_dfs, ignore_index=True)
    df.insert(0, "timestamp_utc", ts_iso)
    
    # 清理数据
    df = clean_dataframe_for_export(df)
    
    # 保存 latest.csv（覆盖）
    df.to_csv(LATEST_CSV, index=False, encoding="utf-8")
    
    # 追加 history.csv（如不存在则新建）
    if not os.path.exists(HISTORY_CSV):
        df.to_csv(HISTORY_CSV, index=False, encoding="utf-8")
    else:
        df.to_csv(HISTORY_CSV, mode="a", header=False, index=False, encoding="utf-8")
    
    print(f"[OK] Wrote {LATEST_CSV} and appended to {HISTORY_CSV}")
    return df


def push_to_google_sheets(df: pd.DataFrame):
    """推送数据到 Google Sheets"""
    if not GSPREAD_AVAILABLE:
        print("[SKIP] gspread not installed, skipping Google Sheets upload")
        return False
    
    if not GOOGLE_SHEETS_CREDENTIALS_JSON:
        print("[SKIP] GOOGLE_SHEETS_CREDENTIALS_JSON not set, skipping Google Sheets upload")
        return False
    
    if not GOOGLE_SHEETS_SPREADSHEET_ID:
        print("[SKIP] GOOGLE_SHEETS_SPREADSHEET_ID not set, skipping Google Sheets upload")
        return False
    
    try:
        # 解析 JSON 凭证
        creds_dict = json.loads(GOOGLE_SHEETS_CREDENTIALS_JSON)
        creds = Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        
        # 连接 Google Sheets
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(GOOGLE_SHEETS_SPREADSHEET_ID)
        
        # 处理 History 工作表（追加新行）
        try:
            history_sheet = spreadsheet.worksheet(GOOGLE_SHEETS_HISTORY_SHEET)
        except gspread.exceptions.WorksheetNotFound:
            # 如果不存在，创建新工作表
            history_sheet = spreadsheet.add_worksheet(
                title=GOOGLE_SHEETS_HISTORY_SHEET,
                rows=1000,
                cols=len(df.columns)
            )
            # 写入表头
            history_sheet.append_row(df.columns.tolist())
        
        # 清理数据，准备推送到 Google Sheets
        df_clean = clean_dataframe_for_export(df)
        
        # 转换为列表，确保所有值都是 JSON 兼容的
        def safe_convert(val):
            """将值转换为 Google Sheets 兼容的格式"""
            if val is None or (isinstance(val, float) and (math.isnan(val) or math.isinf(val))):
                return ""
            if isinstance(val, float):
                # 检查是否为有效的浮点数
                if abs(val) > 1e100:  # 超大数值
                    return ""
            return val
        
        # 转换数据行为列表
        values = []
        for _, row in df_clean.iterrows():
            values.append([safe_convert(val) for val in row.tolist()])
        
        # 追加历史数据行
        if values:
            history_sheet.append_rows(values)
        
        # 处理 Latest 工作表（覆盖最新数据）
        try:
            latest_sheet = spreadsheet.worksheet(GOOGLE_SHEETS_LATEST_SHEET)
            # 清空现有数据
            latest_sheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            # 如果不存在，创建新工作表
            latest_sheet = spreadsheet.add_worksheet(
                title=GOOGLE_SHEETS_LATEST_SHEET,
                rows=100,
                cols=len(df_clean.columns)
            )
        
        # 写入表头和数据
        latest_sheet.append_row(df_clean.columns.tolist())
        if values:
            latest_sheet.append_rows(values)
        
        print(f"[OK] Pushed data to Google Sheets: {GOOGLE_SHEETS_SPREADSHEET_ID}")
        return True
        
    except json.JSONDecodeError as e:
        print(f"[ERROR] Invalid JSON credentials: {e}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"[ERROR] Failed to push to Google Sheets: {e}", file=sys.stderr)
        return False


def main():
    print("[INFO] Starting market data fetch...")
    
    # 抓取数据
    yf_df = fetch_yfinance(YF_TICKERS)
    fred_df = fetch_fred(FRED_SERIES)
    
    # 保存到 CSV
    df = assemble_and_save([yf_df, fred_df])
    
    # 推送到 Google Sheets（如果配置了）
    push_to_google_sheets(df)
    
    print("[INFO] Completed!")


if __name__ == "__main__":
    # 允许本地临时切换 DXY 符号
    if len(sys.argv) > 1 and sys.argv[1] == "--alt-dxy":
        YF_TICKERS["US Dollar Index (DXY)"] = "DX-Y.NYB"
    
    main()

