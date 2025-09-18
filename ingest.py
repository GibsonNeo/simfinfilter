# ingest.py
from pathlib import Path
import pandas as pd, numpy as np, re, yaml
from typing import Dict, List, Tuple
from helpers import extract_date_from_filename

def discover_files(data_dir: str, patterns: dict):
    data = Path(data_dir)
    data.mkdir(parents=True, exist_ok=True)
    files = list(data.glob("*.csv")) + list(data.glob("*.xlsx")) + list(data.glob("*.xls"))
    groups = {}
    for f in files:
        as_of = extract_date_from_filename(f.name, patterns.get("date_regex"))
        if not as_of:
            continue
        # If you sometimes use underscores in dates, normalize them:
        # as_of = as_of.replace("_", "-")
        groups.setdefault(as_of, []).append(f)
    return groups

def _canonicalize_headers(df: pd.DataFrame, registry: dict) -> Tuple[pd.DataFrame, dict, list]:
    mapping = {}
    for canon, meta in registry.items():
        aliases = [str(a).lower() for a in meta.get("aliases", [])]
        found = None
        for col in df.columns:
            if col.lower() == canon.lower() or col.lower() in aliases:
                found = col
                break
        if found:
            mapping[found] = canon
    df2 = df.rename(columns=mapping)
    missing_required = [c for c, meta in registry.items() if meta.get("required") and c not in df2.columns]
    return df2, mapping, missing_required

def _normalize_ids(df: pd.DataFrame, id_cols):
    """Cast merge keys to consistent string dtype and tidy casing/whitespace."""
    df = df.copy()
    for col in id_cols:
        if col in df.columns:
            df[col] = df[col].astype("string")
    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype("string").str.upper().str.strip()
    if "isin" in df.columns:
        df["isin"] = df["isin"].astype("string").str.upper().str.strip()
    if "simfin_id" in df.columns:
        df["simfin_id"] = df["simfin_id"].astype("string").str.strip()
    return df

def load_group_asof(as_of: str, files, config: dict):
    core_reg = config["column_registry"]["core"]
    add_reg  = config["column_registry"]["additional"]
    id_cols = config.get("id_columns", ["simfin_id","ticker","isin"])

    core_df = None
    add_df = None

    core_pref = config["file_patterns"]["core_prefix"]
    add_pref  = config["file_patterns"]["additional_prefix"]

    # Pick up by prefix (fast path)
    for f in files:
        name = f.name
        if name.startswith(core_pref):
            df = pd.read_csv(f) if f.suffix.lower() == ".csv" else pd.read_excel(f)
            df, _, _ = _canonicalize_headers(df, core_reg)
            core_df = df
        elif name.startswith(add_pref):
            df = pd.read_csv(f) if f.suffix.lower() == ".csv" else pd.read_excel(f)
            df, _, _ = _canonicalize_headers(df, add_reg)
            add_df = df

    # Header-based fallback if prefix didn’t match
    if core_df is None:
        for f in files:
            df_try = pd.read_csv(f) if f.suffix.lower() == ".csv" else pd.read_excel(f)
            df_try2, _, _ = _canonicalize_headers(df_try, core_reg)
            if len([c for c in ["simfin_id","ticker","revenues"] if c in df_try2.columns]) >= 2:
                core_df = df_try2
                break

    if add_df is None:
        for f in files:
            df_try = pd.read_csv(f) if f.suffix.lower() == ".csv" else pd.read_excel(f)
            df_try2, _, _ = _canonicalize_headers(df_try, add_reg)
            if len([c for c in ["cash","assets","total_liab"] if c in df_try2.columns]) >= 2:
                add_df = df_try2
                break

    if core_df is None:
        raise ValueError(f"No core sheet found for {as_of}")

    # === NEW: normalize ID columns on both sides to avoid dtype mismatches ===
    core_df = _normalize_ids(core_df, id_cols)
    if add_df is not None:
        add_df = _normalize_ids(add_df, id_cols)

    # Left-join additional on IDs if present
    if add_df is not None:
        # Ensure all id columns exist (even if NA) on both frames
        for key in id_cols:
            if key not in core_df.columns:
                core_df[key] = pd.NA
            if key not in add_df.columns:
                add_df[key] = pd.NA

        on_cols = [c for c in id_cols if c in core_df.columns and c in add_df.columns]

        # Optional debug: uncomment to check dtypes before merging
        # print("DEBUG dtypes core:", {c:str(core_df[c].dtype) for c in on_cols})
        # print("DEBUG dtypes add :", {c:str(add_df[c].dtype)  for c in on_cols})

        if on_cols:
            # validate="m:1" helps catch duplicate keys in additional (comment out if noisy)
            try:
                merged = pd.merge(
                    core_df, add_df, on=on_cols, how="left", suffixes=("","_add"), validate="m:1"
                )
            except Exception:
                # Fallback to plain left merge if validation trips (we can inspect dupes later)
                merged = pd.merge(core_df, add_df, on=on_cols, how="left", suffixes=("","_add"))
        else:
            # Extreme fallback: cartesian (should rarely happen)
            core_df["_tmp_join"] = 1
            add_df["_tmp_join"] = 1
            merged = core_df.merge(add_df, on="_tmp_join", how="left", suffixes=("","_add")).drop(columns=["_tmp_join"])
    else:
        merged = core_df

    merged["as_of"] = as_of
    return merged
