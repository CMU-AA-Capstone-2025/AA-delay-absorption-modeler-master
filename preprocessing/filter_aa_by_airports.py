"""Filter merged carrier summary CSV to only AA flights between a given set of airports.

Usage examples:
  python3 src/filter_aa_by_airports.py --in outputs/US_CARRIER_SUMMARY_MERGED.csv --out outputs/US_AA_10airports.csv

The script streams input in chunks to avoid large memory usage.
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, List, Optional, Set

import pandas as pd


DEFAULT_AIRPORTS = ["DFW", "LGA", "JFK", "PHL", "DCA", "CLT", "MIA", "ORD", "PHX", "LAX"]


def _pick_column(columns: Iterable[str], candidates: Iterable[str]) -> Optional[str]:
    cols = set(c.upper() for c in columns)
    for cand in candidates:
        if cand.upper() in cols:
            # return actual column name (case-sensitive) from original columns
            for c in columns:
                if c.upper() == cand.upper():
                    return c
    return None


def filter_file(
    infile: str,
    outfile: str,
    airports: List[str],
    carrier: str = "AA",
    chunksize: Optional[int] = 200_000,
):
    inpath = Path(infile)
    outpath = Path(outfile)
    if not inpath.exists():
        raise FileNotFoundError(f"Input not found: {inpath}")

    airports_set: Set[str] = set(a.strip().upper() for a in airports)

    # read header to detect columns
    df0 = pd.read_csv(inpath, nrows=0)
    cols = list(df0.columns)

    carrier_col = _pick_column(cols, ["UNIQUE_CARRIER", "OP_UNIQUE_CARRIER", "MKT_UNIQUE_CARRIER", "REPORTING_AIRLINE", "CARRIER"])
    origin_col = _pick_column(cols, ["ORIGIN", "ORIGIN_AIRPORT", "ORIGIN_AIRPORT_ID", "ORIGIN_AIRPORT_SEQ_ID"])
    dest_col = _pick_column(cols, ["DEST", "DEST_AIRPORT", "DEST_AIRPORT_ID", "DEST_AIRPORT_SEQ_ID"])

    if not carrier_col:
        raise KeyError(f"No carrier column found in input. Available columns: {cols}")
    if not origin_col or not dest_col:
        raise KeyError(f"Origin/Dest columns not found. Available columns: {cols}")

    # prepare output
    if outpath.exists():
        outpath.unlink()
    header_written = False

    reader = pd.read_csv(inpath, dtype=str, low_memory=False, chunksize=chunksize) if chunksize else [pd.read_csv(inpath, dtype=str, low_memory=False)]

    for chunk in reader:
        # ensure string columns
        chunk = chunk.astype("string")
        # normalize carrier and airports for comparison
        carr = chunk[carrier_col].fillna("").astype(str).str.strip().str.upper()
        orig = chunk[origin_col].fillna("").astype(str).str.strip().str.upper()
        dst = chunk[dest_col].fillna("").astype(str).str.strip().str.upper()

        mask = (carr == carrier.upper()) & orig.isin(airports_set) & dst.isin(airports_set)

        out_chunk = chunk.loc[mask]
        if out_chunk.empty:
            continue

        out_chunk.to_csv(outpath, mode="a", index=False, header=not header_written)
        header_written = True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="infile", required=True, help="Input merged CSV path")
    ap.add_argument("--out", dest="outfile", required=True, help="Output filtered CSV path")
    ap.add_argument("--airports", default=','.join(DEFAULT_AIRPORTS),
                    help="Comma-separated list of airport IATA codes to keep (both origin and dest).")
    ap.add_argument("--carrier", default="AA", help="Carrier code to keep (default AA)")
    ap.add_argument("--chunksize", type=int, default=200_000, help="pandas read_csv chunksize (0 to disable streaming)")
    args = ap.parse_args()

    airports = [a.strip().upper() for a in args.airports.split(',') if a.strip()]
    chunksize = args.chunksize if args.chunksize and args.chunksize > 0 else None

    filter_file(args.infile, args.outfile, airports, carrier=args.carrier, chunksize=chunksize)


if __name__ == "__main__":
    main()
