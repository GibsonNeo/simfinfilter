#!/usr/bin/env python3
import argparse, yaml, pandas as pd, numpy as np
from pathlib import Path
from ingest import discover_files, load_group_asof
from metrics import compute_metrics
from growth import compute_growth
from grading import prepare_history_aggregates, grade

def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def export_excel(stacked, latest, out_path: Path):
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as xw:
        latest.to_excel(xw, sheet_name="Overview", index=False)
        growth_cols = [c for c in stacked.columns if any(s in c for s in ["_yoy","_cagr2","_cagr3","price","pe"])]
        cols = (["ticker","company","as_of"] + sorted(set(growth_cols)))
        stacked[cols].to_excel(xw, sheet_name="Growth", index=False)

        qual_cols = ["croic_ttm","roe_ttm","roa_ttm","fcf_conversion_ttm","op_margin_ttm","gross_margin_ttm","net_margin_ttm","fcf_margin_ttm","op_margin_ttm_avg_prev2","op_margin_ttm_avg_prev35"]
        present_q = [c for c in qual_cols if c in stacked.columns]
        stacked[["ticker","as_of"]+present_q].to_excel(xw, sheet_name="Quality", index=False)

        bc_cols = ["net_debt","debt_to_equity","current_ratio","liab_to_assets","ocf","chg_wc","net_debt_chg1y","debt_to_equity_chg1y","current_ratio_chg1y","liab_to_assets_chg1y"]
        present_b = [c for c in bc_cols if c in stacked.columns]
        stacked[["ticker","as_of"]+present_b].to_excel(xw, sheet_name="BalanceCash", index=False)

        val_cols = ["pe_ttm","fcf_yield","ev","ebitda","ev_ebitda","mcap","price"]
        present_v = [c for c in val_cols if c in stacked.columns]
        stacked[["ticker","as_of"]+present_v].to_excel(xw, sheet_name="Valuation", index=False)

        grade_cols = [c for c in latest.columns if c.endswith("_score")] + ["overall_grade"]
        add_cols = [c for c in ["sector","industry"] if c in latest.columns]
        latest[["ticker","company","as_of"]+add_cols+grade_cols].to_excel(xw, sheet_name="Grades", index=False)

        audit = stacked.groupby("as_of").size().reset_index(name="rows")
        audit.to_excel(xw, sheet_name="IngestAudit", index=False)

def main():
    ap = argparse.ArgumentParser(description="SimFin Filter — Stateless run")
    ap.add_argument("--config", "-c", default=str(Path(__file__).with_name("config.yml")))
    ap.add_argument("--data_dir", "-d", default=None, help="Override data_dir in config")
    ap.add_argument("--out", "-o", default=None, help="Output Excel path")
    args = ap.parse_args()

    conf = load_config(args.config)
    data_dir = args.data_dir or conf.get("data_dir","./data")
    groups = discover_files(data_dir, conf.get("file_patterns", {}))

    frames = []
    for as_of, files in groups.items():
        merged = load_group_asof(as_of, files, conf)
        frames.append(merged)
    if not frames:
        raise SystemExit("No dated CSV groups found. Check data_dir and filename patterns.")

    all_df = pd.concat(frames, ignore_index=True)
    all_df = compute_metrics(all_df)

    growth_df = compute_growth(all_df, tuple(conf.get("growth_windows", ["1Y","2Y","3Y"])))
    agg_df = prepare_history_aggregates(growth_df)

    agg_df["as_of"] = pd.to_datetime(agg_df["as_of"])
    idx = agg_df.sort_values("as_of").groupby("ticker").tail(1).index
    latest = agg_df.loc[idx].copy()

    # Recency composites
    rw = conf.get("recency",{}).get("growth_weights", {"yoy":0.6,"cagr2":0.3,"cagr3":0.1})
    for prefix in ["rev","ebit","fcf"]:
        arr, wts = [], []
        for k, w in rw.items():
            col = f"{prefix}_{k}"
            if col in latest.columns:
                arr.append(latest[col].astype(float)); wts.append(w)
        if arr:
            import numpy as np
            A = pd.concat(arr, axis=1)
            W = np.array(wts, dtype=float); W = W / W.sum() if W.sum()>0 else W
            latest[f"{prefix}_recency_growth"] = np.nansum(A.values * W.reshape(1,-1), axis=1)

    qrw = conf.get("recency",{}).get("quality_ttm_weights", {"ttm":0.7,"avg_prev_2y":0.2,"avg_prev_3to5y":0.1})
    for m in ["croic_ttm","roe_ttm","roa_ttm","fcf_conversion_ttm","op_margin_ttm"]:
        v = latest.get(m).astype(float)
        p2 = latest.get(m+"_avg_prev2").astype(float)
        p35= latest.get(m+"_avg_prev35").astype(float)
        import numpy as np
        parts = np.vstack([v, p2, p35]).T
        ws = np.array([qrw.get("ttm",0), qrw.get("avg_prev_2y",0), qrw.get("avg_prev_3to5y",0)], dtype=float)
        ws = ws / ws.sum() if ws.sum()>0 else ws
        latest[m] = np.nansum(np.where(np.isnan(parts), np.nanmean(parts, axis=1, keepdims=True), parts) * ws.reshape(1,-1), axis=1)

    brw = conf.get("recency",{}).get("balance_trend_weights", {"level_ttm":0.8,"trend_1y":0.2})
    for m in ["net_debt","debt_to_equity","current_ratio","liab_to_assets"]:
        level = latest.get(m).astype(float)
        trend = latest.get(m+"_chg1y").astype(float)
        import numpy as np
        parts = np.vstack([level, trend]).T
        ws = np.array([brw.get("level_ttm",0), brw.get("trend_1y",0)], dtype=float)
        ws = ws / ws.sum() if ws.sum()>0 else ws
        latest[m] = np.nansum(np.where(np.isnan(parts), np.nanmean(parts, axis=1, keepdims=True), parts) * ws.reshape(1,-1), axis=1)

    graded = grade(latest, conf)

    out_name = conf.get("export",{}).get("excel_filename","Scorebook_{as_of_max}.xlsx")
    as_of_max = agg_df["as_of"].max().strftime("%Y-%m-%d")
    out_name = out_name.format(as_of_max=as_of_max)
    out_path = Path(out_name) if Path(out_name).is_absolute() else Path.cwd() / out_name
    export_excel(agg_df, graded, out_path)
    print(f"Wrote: {out_path}")

if __name__ == "__main__":
    main()
