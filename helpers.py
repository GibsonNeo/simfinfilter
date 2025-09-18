import re, math, numpy as np, pandas as pd

DATE_REGEX_DEFAULT = re.compile(r"(\d{4}-\d{2}-\d{2})")

def extract_date_from_filename(name: str, pattern: str|None=None):
    rx = re.compile(pattern) if pattern else DATE_REGEX_DEFAULT
    m = rx.search(name)
    return m.group(1) if m else None

def safe_div(a, b):
    try:
        if b is None or pd.isna(b) or float(b) == 0.0:
            return np.nan
        return float(a) / float(b)
    except Exception:
        return np.nan

def winsorize(series: pd.Series, low=0.01, high=0.99):
    s = series.copy()
    if s.empty:
        return s
    s = s.clip(lower=s.quantile(low), upper=s.quantile(high))
    return s

def to_percentile(series: pd.Series):
    return series.rank(pct=True, method="average")

def zscore(series: pd.Series):
    s = series.astype(float)
    mu = s.mean()
    sd = s.std(ddof=0)
    if sd == 0 or np.isnan(sd):
        return pd.Series(np.nan, index=s.index)
    return (s - mu) / sd

def pct_change(a, b):
    if b is None or pd.isna(b) or float(b) == 0.0:
        return np.nan
    try:
        return float(a) / float(b) - 1.0
    except Exception:
        return np.nan

def cagr(a, b, years: float):
    if b is None or pd.isna(b) or float(b) <= 0.0 or a is None or pd.isna(a) or float(a) <= 0.0:
        return np.nan
    try:
        return (float(a) / float(b))**(1.0/years) - 1.0
    except Exception:
        return np.nan

def clamp(x, lo, hi):
    try:
        return max(lo, min(hi, float(x)))
    except Exception:
        return np.nan
