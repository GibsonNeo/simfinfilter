# SimFin Filter (Stateless Run)

## What this is
A self-contained Python tool that ingests your TTM CSVs (core + additional), computes growth, ratios, recency-weighted grades, and exports a single Excel scorebook. No external API calls; optional local sector metadata join.

## Layout
- config.yml — edit this first (paths, patterns, weights)
- simfin_filter.py — CLI entry point
- ingest.py, metrics.py, growth.py, grading.py, helpers.py — modules

## Quick start
1. Put your CSVs into a folder (e.g., `./data`) with names like:
   - `portfolio_unsaved_portfolio_2025-09-17.csv`
   - `additional_2025-09-17.csv`
   (and the earlier dates you have)
2. Edit `config.yml` to set `data_dir` to that folder.
3. Run:
   ```bash
   python simfin_filter.py --config config.yml
   ```
4. Output: `Scorebook_YYYY-MM-DD.xlsx` in your current directory.

## Notes
- Sector/industry enrichment is **local CSV only** here (set `sector_industry.local_meta_csv`).
- Recency weighting is built in for growth, quality, and balance trend.
- Valuation pillar uses current PE / EV/EBITDA / FCF Yield.
- This tool is stateless: delete/add CSVs to change what gets computed each run.
