# grading.py  — no SciPy needed
import pandas as pd, numpy as np
from helpers import winsorize, to_percentile, zscore
from math import erf  # builtin

def _standardize(series: pd.Series, method: str):
    if method == "percentile":
        # 0–100 by rank
        return to_percentile(series) * 100.0
    elif method == "zscore":
        # Convert z to ~percentile using error function:
        # Phi(z) = 0.5 * (1 + erf(z / sqrt(2)))
        z = zscore(series)
        # z is a Series; use .apply so we can handle NaNs cleanly
        cdf = 0.5 * (1.0 + z.apply(lambda v: erf(v / np.sqrt(2)) if pd.notna(v) else np.nan))
        return cdf * 100.0
    else:
        return to_percentile(series) * 100.0

def _direction_adjust(values: pd.Series, direction: str):
    return -values if direction == "lower_better" else values

def prepare_history_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["as_of"] = pd.to_datetime(d["as_of"])
    d = d.sort_values(["ticker","as_of"])
    frames = []
    for tkr, g in d.groupby("ticker", dropna=False):
        g = g.sort_values("as_of")
        for i in range(len(g)):
            row = g.iloc[i].copy()
            hist = g.iloc[:i]
            for m in ["croic_ttm","roe_ttm","roa_ttm","fcf_conversion_ttm","op_margin_ttm"]:
                row[m+"_avg_prev2"]  = hist[m].tail(2).mean() if not hist.empty else np.nan
                row[m+"_avg_prev35"] = hist[m].iloc[-5:-2].mean() if len(hist)>=5 else (hist[m].iloc[:-2].tail(3).mean() if len(hist)>2 else np.nan)
            for m in ["net_debt","debt_to_equity","current_ratio","liab_to_assets"]:
                row[m+"_chg1y"] = row.get(m) - (hist.iloc[-1].get(m) if len(hist)>=1 else np.nan)
            frames.append(row)
    return pd.DataFrame(frames)

def grade(df_latest: pd.DataFrame, config: dict, sector: bool = False) -> pd.DataFrame:
    d = df_latest.copy()
    gconf = config["grading"]
    wins = gconf.get("winsor", {"p_low":0.01,"p_high":0.99})
    standardize_method = gconf.get("standardize","percentile")

    # Which metrics are “lower is better”
    direction = gconf.get("pillars",{}).get("balance",{}).get("direction",{})
    val_dir   = gconf.get("pillars",{}).get("valuation",{}).get("direction",{})
    metric_dir = {**direction, **val_dir}

    # Winsorize + direction-adjust + standardize each metric
    for pillar_name, pconf in gconf["pillars"].items():
        for m in pconf.get("metrics", []):
            if m not in d.columns:
                continue
            s = d[m].astype(float)
            s = winsorize(s, low=wins["p_low"], high=wins["p_high"])
            s = _direction_adjust(s, metric_dir.get(m, "higher_better"))
            d[m+"_std"] = _standardize(s, standardize_method)

    # Pillar scores = mean of standardized metrics in the pillar
    pillar_weights = {}
    for pillar_name, pconf in gconf["pillars"].items():
        cols = [c+"_std" for c in pconf.get("metrics", []) if c+"_std" in d.columns]
        d[pillar_name+"_score"] = d[cols].mean(axis=1) if cols else np.nan
        pillar_weights[pillar_name] = pconf.get("weight", 0.0)

    # Overall grade = weighted sum of pillar scores
    total_w = sum(pillar_weights.values()) or 1.0
    d["overall_grade"] = 0.0
    for name, w in pillar_weights.items():
        d["overall_grade"] = d["overall_grade"] + (w / total_w) * d.get(name+"_score", np.nan)

    return d
