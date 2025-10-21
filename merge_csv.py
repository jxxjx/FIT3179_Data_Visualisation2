# merge_tourism_gdp.py
import pandas as pd

# === File paths (edit if needed) ===
TOURISM_CSV   = "foreign_arrivals.csv"                   # poe,country,date,arrivals,arrivals_male,arrivals_female
GDP_CSV       = "gdp_qtr_real_supply_sub.csv"   # series,date,sector,value (quarterly)
SECTORS_CSV   = "gdp_tourism_related.csv"           # method,sector,desc_en (your list)
OUT_CSV       = "merged_growth_tourism_gdp.csv"

# === Load ===
tourism = pd.read_csv(TOURISM_CSV)
gdp     = pd.read_csv(GDP_CSV)
sectors = pd.read_csv(SECTORS_CSV)

# Parse dates
tourism["date"] = pd.to_datetime(tourism["date"])
gdp["date"]     = pd.to_datetime(gdp["date"])

# Normalize column names (defensive)
for df in (tourism, gdp, sectors):
    df.columns = [c.strip().lower() for c in df.columns]

# --- Keep only tourism-related GDP sectors (by 'sector' code) ---
tourism_sector_codes = set(sectors["sector"].astype(str).str.strip())
gdp["sector"] = gdp["sector"].astype(str).str.strip()

# (Optional) keep production method only if present in your GDP file
if "method" in gdp.columns and "method" in sectors.columns:
    gdp = gdp.merge(sectors[["sector","method"]].drop_duplicates(), on="sector", how="left")
    # prefer rows that match sector+method in your mapping
    gdp = gdp[(gdp["sector"].isin(tourism_sector_codes)) & (gdp["method"].eq("production"))]
else:
    gdp = gdp[gdp["sector"].isin(tourism_sector_codes)]

# Attach human-readable descriptions (desc_en)
gdp = gdp.merge(sectors[["sector","desc_en"]].drop_duplicates(), on="sector", how="left")

# --- Quarter alignment using period (avoids timestamp mismatches) ---
tourism["qtr"] = tourism["date"].dt.to_period("Q-DEC")
gdp["qtr"]     = gdp["date"].dt.to_period("Q-DEC")

# --- Aggregate tourism arrivals to quarter (sum across POE & countries) ---
tourism_qtr = (
    tourism.groupby("qtr", as_index=False)
           .agg(tourism_arrivals=("arrivals", "sum"))
)

# --- Aggregate tourism-only GDP by quarter ---
# If GDP values are already quarterly, we just sum across the selected tourism sectors
gdp_tourism_qtr = (
    gdp.groupby("qtr", as_index=False)
       .agg(gdp_tourism_value=("value", "sum"))
)

# --- Merge on quarter ---
merged = (
    pd.merge(gdp_tourism_qtr, tourism_qtr, on="qtr", how="inner")
      .sort_values("qtr")
      .reset_index(drop=True)
)

# --- QoQ growth (%) ---
merged["gdp_tourism_growth_%"] = merged["gdp_tourism_value"].pct_change() * 100.0
merged["tourism_growth_%"]     = merged["tourism_arrivals"].pct_change() * 100.0

# Add a concrete quarter-end date for plotting/tooltips
merged["date"] = merged["qtr"].dt.to_timestamp(how="end")

# Reorder columns
merged = merged[[
    "qtr", "date",
    "gdp_tourism_value", "tourism_arrivals",
    "gdp_tourism_growth_%", "tourism_growth_%"
]]

print(merged.head(10))
merged.to_csv(OUT_CSV, index=False)
print(f"\nSaved: {OUT_CSV}")
