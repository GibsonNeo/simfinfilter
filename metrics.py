import pandas as pd, numpy as np, math
from helpers import safe_div, clamp

def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()

    d["fcf_margin_ttm"] = d.apply(lambda r: safe_div(r.get("fcf"), r.get("revenues")), axis=1)
    d["fcf_conversion_ttm"] = d.apply(lambda r: safe_div(r.get("fcf"), r.get("net_income")), axis=1)

    d["invested_capital"] = d.get("total_debt", np.nan) + d.get("equity", np.nan) - d.get("cash", np.nan)
    tr = d.apply(lambda r: safe_div(r.get("taxes"), r.get("ebit")), axis=1)
    d["tax_rate"] = tr.clip(lower=0.0, upper=0.5)
    d["nopat"] = d.get("ebit", np.nan) * (1 - d["tax_rate"])
    d["roic_ttm"] = d.apply(lambda r: safe_div(r.get("nopat"), r.get("invested_capital")), axis=1)

    d["net_debt"] = d.get("total_debt", np.nan) - d.get("cash", np.nan)
    d["debt_to_equity"] = d.apply(lambda r: safe_div(r.get("total_debt"), r.get("equity")), axis=1)
    d["current_ratio"] = d.apply(lambda r: safe_div(r.get("current_assets"), r.get("current_liab")), axis=1)
    d["liab_to_assets"] = d.apply(lambda r: safe_div(r.get("total_liab"), r.get("assets")), axis=1)

    d["ev"] = d.get("mcap", np.nan) + d.get("total_debt", np.nan) - d.get("cash", np.nan)
    d["ev_ebitda"] = d.apply(lambda r: safe_div(r.get("ev"), r.get("ebitda")), axis=1)
    d["fcf_yield"] = d.apply(lambda r: safe_div(r.get("fcf"), r.get("mcap")), axis=1)

    d["op_margin_ttm"] = d.get("op_margin", np.nan)
    d["gross_margin_ttm"] = d.get("gross_margin", np.nan)
    d["net_margin_ttm"] = d.get("net_margin", np.nan)
    d["roe_ttm"] = d.get("roe", np.nan)
    d["roa_ttm"] = d.get("roa", np.nan)
    d["croic_ttm"] = d.get("croic", np.nan)
    d["pe_ttm"] = d.get("pe", np.nan)

    return d
