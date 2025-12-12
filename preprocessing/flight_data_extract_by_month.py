#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch American Airlines (AA) domestic flights' timing fields from
BTS TranStats On-Time monthly ZIPs for given years (default 2023–2024),
with robust column-name normalization and streaming (chunksize) processing.

Key fixes:
- Support 'On_Time_Reporting_Carrier' schema: Reporting_Airline / IATA_CODE_Reporting_Airline
- Streaming reader consumed inside the same ZipFile scope (PlanA)
"""

from __future__ import annotations
import argparse
import io
import sys
import time
import zipfile
import re
from typing import Optional, List

import pandas as pd
import requests
from tqdm import tqdm

# -----------------------------
# Config & constants
# -----------------------------

BASE = "https://transtats.bts.gov/PREZIP"

# Observed filename patterns
NAME_PATTERNS = [
    "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip",
    "On_Time_Reporting_Carrier_On-Time_Performance_(1987_present)_{year}_{month}.zip",
    "On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_{year}_{month}.zip",
]

# Output columns (include both OP & MKT to handle different schemas)
OUTPUT_COLS = [
    "FL_DATE",
    "OP_UNIQUE_CARRIER",
    "MKT_UNIQUE_CARRIER",
    "OP_CARRIER_FL_NUM",
    "ORIGIN", "DEST",
    "CRS_DEP_TIME", "DEP_TIME", "WHEELS_OFF",
    "CRS_ARR_TIME", "ARR_TIME", "WHEELS_ON",
    "CANCELLED", "DIVERTED",
]

# Treat US territories as domestic by default
US_TERRITORY_STATE_ABR = {"PR", "VI", "GU", "AS", "MP"}

# Column alias normalization map
CANON_MAP = {
    # Date
    "FL_DATE": ["FL_DATE", "FLIGHT_DATE", "FLIGHTDATE"],
    # Carrier (operating/marketing/reporting)
    "OP_UNIQUE_CARRIER": [
        "OP_UNIQUE_CARRIER", "OPERATING_AIRLINE", "OPERATINGCARRIER", "OP_CARRIER",
        "OPERATING_AIRLINE_IATA_CODE", "OPERATINGAIRLINEIATACODE"
    ],
    "MKT_UNIQUE_CARRIER": [
        "MKT_UNIQUE_CARRIER", "MARKETING_AIRLINE_NETWORK", "MARKETINGCARRIER",
        "MKT_CARRIER", "MKTUNIQUECARRIER",
        # Reporting Carrier (most common in 'On_Time_Reporting_Carrier' exports)
        "REPORTING_AIRLINE", "IATA_CODE_REPORTING_AIRLINE"
    ],
    # Flight number (use reporting/operating variants)
    "OP_CARRIER_FL_NUM": [
        "OP_CARRIER_FL_NUM", "OP_CARRIER_FLNUM",
        "FL_NUM", "FLIGHT_NUM", "FLIGHT_NUMBER",
        "FLIGHT_NUMBER_REPORTING_AIRLINE", "FLIGHT_NUMBER_OPERATING_AIRLINE",
        "FLIGHT_NUMBER_REPORTINGAIRLINE", "FLIGHT_NUMBER_OPERATINGAIRLINE"
    ],
    # Airports
    "ORIGIN": ["ORIGIN", "ORIGINAIRPORT", "ORIGIN_AIRPORT"],
    "DEST": ["DEST", "DESTAIRPORT", "DEST_AIRPORT"],
    # States / Country (BTS has 'OriginState'/'DestState' and sometimes country names)
    "ORIGIN_STATE_ABR": ["ORIGIN_STATE_ABR", "ORIGIN_STATE_ABBREVIATION", "ORIGINSTATE", "ORIGIN_STATE"],
    "DEST_STATE_ABR": ["DEST_STATE_ABR", "DEST_STATE_ABBREVIATION", "DESTSTATE", "DEST_STATE"],
    "ORIGIN_COUNTRY_NAME": ["ORIGIN_COUNTRY_NAME", "ORIGINCOUNTRYNAME", "ORIGIN_COUNTRY"],
    "DEST_COUNTRY_NAME": ["DEST_COUNTRY_NAME", "DESTCOUNTRYNAME", "DEST_COUNTRY"],
    # Timing fields
    "CRS_DEP_TIME": ["CRS_DEP_TIME", "CRSDEPTIME"],
    "DEP_TIME": ["DEP_TIME", "DEPTIME"],
    "WHEELS_OFF": ["WHEELS_OFF", "WHEELSOFF"],
    "CRS_ARR_TIME": ["CRS_ARR_TIME", "CRSARRTIME"],
    "ARR_TIME": ["ARR_TIME", "ARRTIME"],
    "WHEELS_ON": ["WHEELS_ON", "WHEELSON"],
    # Others
    "CANCELLED": ["CANCELLED", "CANCELLED_"],
    "DIVERTED": ["DIVERTED", "DIVERTED_"],
}

UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; AA-OTP-collector/1.1; +https://example.org)"
}

# -----------------------------
# Helpers
# -----------------------------

def _norm(s: str) -> str:
    """Normalize column name for alias matching: uppercase & remove non [A-Z0-9]."""
    return re.sub(r"[^A-Z0-9]", "", s.upper())

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns to canonical names using CANON_MAP."""
    norm_to_actual = {_norm(c): c for c in df.columns}
    rename_map = {}
    for canon, aliases in CANON_MAP.items():
        for a in aliases:
            n = _norm(a)
            if n in norm_to_actual:
                rename_map[norm_to_actual[n]] = canon
                break
    if rename_map:
        df = df.rename(columns=rename_map)
    return df

def month_url(year: int, month: int, timeout: int = 20) -> Optional[str]:
    """Try HEAD multiple known patterns to locate the monthly ZIP URL."""
    for pat in NAME_PATTERNS:
        url = f"{BASE}/{pat.format(year=year, month=month)}"
        try:
            r = requests.head(url, timeout=timeout, allow_redirects=True, headers=UA_HEADERS)
            if r.status_code == 200 and int(r.headers.get("Content-Length", "1")) > 1:
                return url
            if r.status_code == 200 and "zip" in r.headers.get("Content-Type", "").lower():
                return url
        except requests.RequestException:
            continue
    return None

def download_zip(url: str, retries: int = 3, timeout: int = 60) -> Optional[bytes]:
    """Download the ZIP file with retries; return bytes or None."""
    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, stream=True, timeout=timeout, headers=UA_HEADERS) as r:
                r.raise_for_status()
                buf = io.BytesIO()
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        buf.write(chunk)
                return buf.getvalue()
        except requests.RequestException as e:
            print(f"[WARN] Download failed ({attempt}/{retries}): {e}")
            if attempt < retries:
                time.sleep(1.5 * attempt)
    return None

def filter_domestic(df: pd.DataFrame, include_territories: bool = True) -> pd.DataFrame:
    """
    Keep domestic flights:
    Priority 1: ORIGIN_COUNTRY_NAME & DEST_COUNTRY_NAME == 'UNITED STATES'
    Fallback  : ORIGIN_STATE_ABR & DEST_STATE_ABR present (two-letter code). Optionally include territories.
    """
    df = standardize_columns(df)

    if "ORIGIN_COUNTRY_NAME" in df.columns and "DEST_COUNTRY_NAME" in df.columns:
        mask = (df["ORIGIN_COUNTRY_NAME"] == "UNITED STATES") & (df["DEST_COUNTRY_NAME"] == "UNITED STATES")
        return df.loc[mask]

    if "ORIGIN_STATE_ABR" in df.columns and "DEST_STATE_ABR" in df.columns:
        mask = df["ORIGIN_STATE_ABR"].notna() & df["DEST_STATE_ABR"].notna()
        if include_territories:
            return df.loc[mask]
        else:
            mask2 = ~df["ORIGIN_STATE_ABR"].isin(US_TERRITORY_STATE_ABR) & ~df["DEST_STATE_ABR"].isin(US_TERRITORY_STATE_ABR)
            return df.loc[mask & mask2]

    # Last resort: don't filter (most "Reporting Carrier" tables are domestic anyway)
    return df

def keep_aa(df: pd.DataFrame, use_marketing: bool) -> pd.DataFrame:
    """
    Filter AA by operating or marketing carrier with robust fallback.
    For 'On_Time_Reporting_Carrier' tables, MKT_UNIQUE_CARRIER (Reporting_Airline) is available.
    """
    df = standardize_columns(df)
    primary = "MKT_UNIQUE_CARRIER" if use_marketing else "OP_UNIQUE_CARRIER"
    # Try primary, else fall back to the other, else fall back to MKT as reporting
    if primary in df.columns:
        col = primary
    else:
        candidates = []
        if primary != "MKT_UNIQUE_CARRIER":
            candidates.append("MKT_UNIQUE_CARRIER")
        if primary != "OP_UNIQUE_CARRIER":
            candidates.append("OP_UNIQUE_CARRIER")
        col = next((c for c in candidates if c in df.columns), None)

    if not col:
        # Show available columns to help debug
        raise KeyError(f"Missing carrier column; have: {list(df.columns)}")

    return df.loc[df[col] == "AA"]

def select_output_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    cols = [c for c in OUTPUT_COLS if c in df.columns]
    if not cols:
        return pd.DataFrame()
    out = df.loc[:, cols].copy()
    # times as string
    for tcol in ["CRS_DEP_TIME","DEP_TIME","WHEELS_OFF","CRS_ARR_TIME","ARR_TIME","WHEELS_ON"]:
        if tcol in out.columns:
            out[tcol] = out[tcol].astype("string")
    if "OP_CARRIER_FL_NUM" in out.columns:
        out["OP_CARRIER_FL_NUM"] = out["OP_CARRIER_FL_NUM"].astype("string")
    return out

# -----------------------------
# Core per-month processor (Plan A)
# -----------------------------

def process_month(
    year: int,
    month: int,
    use_marketing: bool,
    chunksize: Optional[int],
    include_territories: bool,
    timeout: int,
    retries: int,
) -> pd.DataFrame:
    url = month_url(year, month, timeout=timeout)
    if not url:
        print(f"[INFO] Not found: {year}-{month:02d} (skipped)")
        return pd.DataFrame()

    print(f"[INFO] Downloading {url}")
    zbytes = download_zip(url, retries=retries, timeout=timeout)
    if not zbytes:
        print(f"[WARN] Empty download for {year}-{month:02d}")
        return pd.DataFrame()

    parts: List[pd.DataFrame] = []

    with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
        name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), None)
        if not name:
            print(f"[WARN] No CSV found in ZIP for {year}-{month:02d}")
            return pd.DataFrame()
        with zf.open(name) as f:
            if chunksize:
                reader = pd.read_csv(f, low_memory=False, dtype=str, chunksize=chunksize)
                for chunk in tqdm(reader, desc=f"Filtering {year}-{month:02d}"):
                    chunk = standardize_columns(chunk)
                    chunk = filter_domestic(chunk, include_territories=include_territories)
                    chunk = keep_aa(chunk, use_marketing=use_marketing)
                    chunk = select_output_cols(chunk)
                    if not chunk.empty:
                        parts.append(chunk)
            else:
                df = pd.read_csv(f, low_memory=False, dtype=str)
                df = standardize_columns(df)
                df = filter_domestic(df, include_territories=include_territories)
                df = keep_aa(df, use_marketing=use_marketing)
                df = select_output_cols(df)
                if not df.empty:
                    parts.append(df)

    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)

# -----------------------------
# Appending writer (optional)
# -----------------------------

def append_write(df: pd.DataFrame, out_path: str, header_written: bool) -> bool:
    if df.empty:
        return header_written
    df.to_csv(out_path, mode="a", index=False, header=not header_written)
    return True

# -----------------------------
# Main
# -----------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="Output CSV path")
    ap.add_argument("--years", nargs="+", type=int, default=[2023, 2024], help="Years to download")
    ap.add_argument("--use_marketing", action="store_true",
                    help="Filter AA by marketing carrier (MKT_UNIQUE_CARRIER). Default uses operating if available.")
    ap.add_argument("--chunksize", type=int, default=300_000, help="pandas read_csv chunksize (None loads per-month fully)")
    ap.add_argument("--append_out", action="store_true", help="Append month-by-month to the output CSV (lower memory).")
    ap.add_argument("--include_territories", action="store_true", default=True,
                    help="Treat US territories (PR/VI/GU/AS/MP) as domestic (default True).")
    ap.add_argument("--timeout", type=int, default=60, help="HTTP timeout seconds")
    ap.add_argument("--retries", type=int, default=3, help="HTTP retry attempts")
    args = ap.parse_args()

    if args.append_out:
        open(args.out, "w").close()
        header_written = False
        for y in args.years:
            for m in range(1, 13):
                try:
                    dfm = process_month(
                        y, m,
                        use_marketing=args.use_marketing,
                        chunksize=args.chunksize,
                        include_territories=args.include_territories,
                        timeout=args.timeout,
                        retries=args.retries,
                    )
                    header_written = append_write(dfm, args.out, header_written)
                except Exception as e:
                    print(f"[WARN] Failed {y}-{m:02d}: {e}", file=sys.stderr)
        print(f"[OK] Appended CSV written → {args.out}")
        return

    frames: List[pd.DataFrame] = []
    for y in args.years:
        for m in range(1, 13):
            try:
                dfm = process_month(
                    y, m,
                    use_marketing=args.use_marketing,
                    chunksize=args.chunksize,
                    include_territories=args.include_territories,
                    timeout=args.timeout,
                    retries=args.retries,
                )
                if not dfm.empty:
                    frames.append(dfm)
            except Exception as e:
                print(f"[WARN] Failed {y}-{m:02d}: {e}", file=sys.stderr)

    if not frames:
        print("[ERROR] No data collected. Check connectivity or months availability.")
        sys.exit(2)

    all_df = pd.concat(frames, ignore_index=True)
    all_df.to_csv(args.out, index=False)
    print(f"[OK] Wrote {len(all_df):,} rows → {args.out}")

if __name__ == "__main__":
    main()
