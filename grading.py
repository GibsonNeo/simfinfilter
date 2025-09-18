import pandas as pd, numpy as np
from helpers import winsorize, to_percentile, zscore

def _standardize(series: pd.Series, method: str):
    if method == "percentile":
        return to_percentile(series) * 100.0
    elif method == "zscore":
        s = zscore(series)
        # Convert z to ~percentile scale (approx)
        from scipy.stats import norm
        return pd.Series(norm.cdf(s), index=s.index) * 100.0
    else:
        return to_percentile(series) * 100.0

def _direction_adjust(values: pd.Series, direction: str):
    if direction == "lower_better":
        return -values
    return values

def prepare_history_aggregates(df):
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
                row[m+"_avg_prev2"] = hist[m].tail(2).mean() if not hist.empty else np.nan
                row[m+"_avg_prev35"] = hist[m].iloc[-5:-2].mean() if len(hist)>=5 else (hist[m].iloc[:-2].tail(3).mean() if len(hist)>2 else np.nan)
            for m in ["net_debt","debt_to_equity","current_ratio","liab_to_assets"]:
                row[m+"_chg1y"] = row.get(m) - (hist.iloc[-1].get(m) if len(hist)>=1 else np.nan)
            frames.append(row)
    return pd.DataFrame(frames)

def grade(df_latest: pd.DataFrame, config: dict, sector=False) -> pd.DataFrame:
    d = df_latest.copy()
    gconf = config["grading"]
    wins = gconf.get("winsor", {"p_low":0.01,"p_high":0.99})
    standardize_method = gconf.get("standardize","percentile")

    direction = gconf.get("pillars",{}).get("balance",{}).get("direction",{})
    val_dir = gconf.get("pillars",{}).get("valuation",{}).get("direction",{})
    metric_dir = {**direction, **val_dir}

    for pillar_name, pconf in gconf["pillars"].items():
        for m in pconf.get("metrics", []):
            if m not in d.columns: 
                continue
            series = d[m].astype(float)
            series = winsorize(series, low=wins["p_low"], high=wins["p_high"])
            series = _direction_adjust(series, metric_dir.get(m,"higher_better"))
            d[m+"_std"] = _standardize(series, standardize_method)

    pillar_scores = {}
    for pillar_name, pconf in gconf["pillars"].items():
        cols = [c+"_std" for c in pconf.get("metrics", []) if c+"_std" in d.columns]
        d[pillar_name+"_score"] = d[cols].mean(axis=1) if cols else np.nan
        pillar_scores[pillar_name] = pconf.get("weight",0)

    total_w = sum(pillar_scores.values()) or 1.0
    d["overall_grade"] = 0.0
    for name, w in pillar_scores.items():
        d["overall_grade"] = d["overall_grade"] + (w/total_w) * d.get(name+"_score", np.nan)

    return d
