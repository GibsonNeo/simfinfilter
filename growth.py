import pandas as pd, numpy as np
from helpers import pct_change, cagr

def compute_growth(stacked: pd.DataFrame, windows=("1Y","2Y","3Y")) -> pd.DataFrame:
    d = stacked.copy()
    d["as_of"] = pd.to_datetime(d["as_of"])
    d = d.sort_values(["ticker","as_of"])

    metrics = ["revenues","ebit","net_income","fcf","price","pe"]

    out_rows = []
    for key, g in d.groupby("ticker", dropna=False):
        g = g.sort_values("as_of")
        for idx in range(len(g)):
            row = g.iloc[idx].copy()
            for m in metrics:
                val_t0 = g.iloc[idx].get(m)
                if "1Y" in windows and idx-1 >= 0:
                    row[f"{m}_yoy"] = pct_change(val_t0, g.iloc[idx-1].get(m))
                if "2Y" in windows and idx-2 >= 0:
                    row[f"{m}_cagr2"] = cagr(val_t0, g.iloc[idx-2].get(m), 2.0)
                if "3Y" in windows and idx-3 >= 0:
                    row[f"{m}_cagr3"] = cagr(val_t0, g.iloc[idx-3].get(m), 3.0)
            if "shares_basic" in g.columns:
                if idx-1 >= 0:
                    row["shares_dilution_1Y"] = pct_change(g.iloc[idx].get("shares_basic"), g.iloc[idx-1].get("shares_basic"))
                if idx-2 >= 0:
                    row["shares_dilution_2Y"] = cagr(g.iloc[idx].get("shares_basic"), g.iloc[idx-2].get("shares_basic"), 2.0)
                if idx-3 >= 0:
                    row["shares_dilution_3Y"] = cagr(g.iloc[idx].get("shares_basic"), g.iloc[idx-3].get("shares_basic"), 3.0)
            out_rows.append(row)
    return pd.DataFrame(out_rows)
