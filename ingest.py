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
        groups.setdefault(as_of, []).append(f)
    return groups

def _canonicalize_headers(df: pd.DataFrame, registry: dict) -> Tuple[pd.DataFrame, dict, list]:
    mapping = {}
    for canon, meta in registry.items():
        aliases = [str(a).lower() for a in meta.get("aliases", [])]
        found = None
        for col in df.columns:
            if col.lower() == canon.lower() or col.lower() in aliases:
                found = col; break
        if found:
            mapping[found] = canon
    df2 = df.rename(columns=mapping)
    missing_required = [c for c, meta in registry.items() if meta.get("required") and c not in df2.columns]
    return df2, mapping, missing_required

def load_group_asof(as_of: str, files, config: dict):
    core_reg = config["column_registry"]["core"]
    add_reg  = config["column_registry"]["additional"]
    id_cols = config.get("id_columns", ["simfin_id","ticker","isin"])

    core_df = None
    add_df = None

    core_pref = config["file_patterns"]["core_prefix"]
    add_pref  = config["file_patterns"]["additional_prefix"]

    for f in files:
        name = f.name
        if name.startswith(core_pref):
            df = pd.read_csv(f) if f.suffix.lower()==".csv" else pd.read_excel(f)
            df, _, _ = _canonicalize_headers(df, core_reg)
            core_df = df
        elif name.startswith(add_pref):
            df = pd.read_csv(f) if f.suffix.lower()==".csv" else pd.read_excel(f)
            df, _, _ = _canonicalize_headers(df, add_reg)
            add_df = df

    if core_df is None:
        for f in files:
            df_try = pd.read_csv(f) if f.suffix.lower()==".csv" else pd.read_excel(f)
            df_try2, _, _ = _canonicalize_headers(df_try, core_reg)
            if len([c for c in ["simfin_id","ticker","revenues"] if c in df_try2.columns])>=2:
                core_df = df_try2; break

    if add_df is None:
        for f in files:
            df_try = pd.read_csv(f) if f.suffix.lower()==".csv" else pd.read_excel(f)
            df_try2, _, _ = _canonicalize_headers(df_try, add_reg)
            if len([c for c in ["cash","assets","total_liab"] if c in df_try2.columns])>=2:
                add_df = df_try2; break

    if core_df is None:
        raise ValueError(f"No core sheet found for {as_of}")

    if add_df is not None:
        for key in id_cols:
            if key not in core_df.columns: core_df[key] = pd.NA
            if key not in add_df.columns: add_df[key] = pd.NA
        on_cols = [c for c in id_cols if c in core_df.columns and c in add_df.columns]
        if on_cols:
            merged = pd.merge(core_df, add_df, on=on_cols, how="left", suffixes=("","_add"))
        else:
            core_df["_tmp_join"] = 1; add_df["_tmp_join"] = 1
            merged = core_df.merge(add_df, on="_tmp_join", how="left", suffixes=("","_add")).drop(columns=["_tmp_join"])
    else:
        merged = core_df

    merged["as_of"] = as_of
    return merged
