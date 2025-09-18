#!/usr/bin/env python3
# pull_simfin_bulk_cfg.py
#
# Usage:
#   export SIMFIN_API_KEY=your_key
#   python pull_simfin_bulk_cfg.py --config config.yml
#   # CLI flags override config:
#   python pull_simfin_bulk_cfg.py --config config.yml --years 7 --market de
#
# pip install simfin pandas pyyaml

import os
import argparse
import yaml
import pandas as pd
import simfin as sf
from simfin.names import REPORT_DATE, PUBLISH_DATE

DEFAULTS = {
    "api_key_env": "SIMFIN_API_KEY",
    "data_dir": "data",
    "years": 5,
    "market": "us",
    "datasets": {
        "fundamentals_quarterly": True,
        "fundamentals_annual": True,
        "prices_daily": True,
        "companies_meta": True,
        "shares_meta": True,
    },
    "export_last_n_years": True,
}

DATASETS = [
    ("income",   "quarterly"),
    ("balance",  "quarterly"),
    ("cashflow", "quarterly"),
    ("income",   "annual"),
    ("balance",  "annual"),
    ("cashflow", "annual"),
    ("companies","all"),
    ("shares",   "all"),
    ("shareprices","daily"),
]

def deep_merge(a: dict, b: dict) -> dict:
    out = dict(a)
    for k, v in (b or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out

def load_config(path: str | None) -> dict:
    cfg = dict(DEFAULTS)
    if path:
        with open(path, "r") as f:
            file_cfg = yaml.safe_load(f) or {}
        cfg = deep_merge(cfg, file_cfg)
    return cfg

def apply_cli_overrides(cfg: dict, args) -> dict:
    if args.data_dir: cfg["data_dir"] = args.data_dir
    if args.years is not None: cfg["years"] = args.years
    if args.market: cfg["market"] = args.market
    return cfg

def load_dataset(dataset: str, variant: str, market: str):
    try:
        if dataset in {"companies", "shares"}:
            return sf.load(dataset=dataset, market=market)
        if dataset == "shareprices":
            return sf.load(dataset=dataset, variant=variant, market=market)
        return sf.load(dataset=dataset, variant=variant, market=market)
    except Exception as e:
        print(f"[skip] {dataset} {variant} {market}: {e}")
        return None

def export_last_n_years(df: pd.DataFrame, years: int, date_col: str, out_csv: str):
    if df is None or df.empty:
        return
    if date_col not in df.columns:
        # try a few common fallbacks
        for c in ("Report Date", "Date", "report-date", "report_date"):
            if c in df.columns:
                date_col = c
                break
    if date_col not in df.columns:
        print(f"[warn] no date column for {out_csv}, writing full dataset.")
        df.to_csv(out_csv, index=False)
        return

    d = df.copy()
    d[date_col] = pd.to_datetime(d[date_col], errors="coerce")
    cutoff = pd.Timestamp.utcnow().tz_localize(None) - pd.DateOffset(years=years)
    d = d[d[date_col] >= cutoff].copy()
    d.to_csv(out_csv, index=False)
    print(f"[ok] wrote {out_csv} rows={len(d)}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None, help="path to config.yml")
    ap.add_argument("--data-dir", default=None, help="override data dir")
    ap.add_argument("--years", type=int, default=None, help="override last N years to export")
    ap.add_argument("--market", default=None, choices=["us","de","all"], help="override market")
    args = ap.parse_args()

    cfg = load_config(args.config)
    cfg = apply_cli_overrides(cfg, args)

    key = os.getenv(cfg["api_key_env"], "").strip()
    if not key:
        raise SystemExit(f"Set {cfg['api_key_env']} in your environment.")

    sf.set_data_dir(cfg["data_dir"])
    sf.set_api_key(key)

    ds_flags = cfg["datasets"]
    want_q = ds_flags.get("fundamentals_quarterly", True)
    want_a = ds_flags.get("fundamentals_annual", True)
    want_px = ds_flags.get("prices_daily", True)
    want_comp = ds_flags.get("companies_meta", True)
    want_sh = ds_flags.get("shares_meta", True)

    markets = ["us"] if cfg["market"] != "all" else ["us","de"]

    for mkt in markets:
        print(f"\n=== Market: {mkt.upper()} ===")
        loaded = {}

        for dataset, variant in DATASETS:
            if dataset in ("income","balance","cashflow") and variant == "quarterly" and not want_q:
                continue
            if dataset in ("income","balance","cashflow") and variant == "annual" and not want_a:
                continue
            if dataset == "shareprices" and not want_px:
                continue
            if dataset == "companies" and not want_comp:
                continue
            if dataset == "shares" and not want_sh:
                continue

            print(f"loading {dataset} {variant} ...")
            df = load_dataset(dataset, variant, mkt)
            loaded[(dataset, variant)] = df
            if df is not None:
                print(f"[ok] {dataset} {variant}: shape={df.shape}")
            else:
                print(f"[skip] {dataset} {variant}")

        if cfg.get("export_last_n_years", True):
            outdir = os.path.join(cfg["data_dir"], "exports", mkt)
            os.makedirs(outdir, exist_ok=True)
            # quarterly
            export_last_n_years(loaded.get(("income","quarterly")),   cfg["years"], REPORT_DATE,  os.path.join(outdir, "income_quarterly_lastN.csv"))
            export_last_n_years(loaded.get(("balance","quarterly")),  cfg["years"], REPORT_DATE,  os.path.join(outdir, "balance_quarterly_lastN.csv"))
            export_last_n_years(loaded.get(("cashflow","quarterly")), cfg["years"], REPORT_DATE,  os.path.join(outdir, "cashflow_quarterly_lastN.csv"))
            # annual
            export_last_n_years(loaded.get(("income","annual")),   cfg["years"], REPORT_DATE,  os.path.join(outdir, "income_annual_lastN.csv"))
            export_last_n_years(loaded.get(("balance","annual")),  cfg["years"], REPORT_DATE,  os.path.join(outdir, "balance_annual_lastN.csv"))
            export_last_n_years(loaded.get(("cashflow","annual")), cfg["years"], REPORT_DATE,  os.path.join(outdir, "cashflow_annual_lastN.csv"))
            # prices
            export_last_n_years(loaded.get(("shareprices","daily")), cfg["years"], PUBLISH_DATE, os.path.join(outdir, "prices_daily_lastN.csv"))

        print("\nSummary")
        for (ds, var), df in loaded.items():
            shape = df.shape if df is not None else (0, 0)
            print(f"  {ds:11s} {var:10s} -> {shape}")

if __name__ == "__main__":
    main()
