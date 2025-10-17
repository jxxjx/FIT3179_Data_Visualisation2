import pandas as pd

# =========================================================
# 1️⃣ Load input files
# =========================================================
sectors = pd.read_csv("gdp_tourism_related.csv")                     # method, sector, desc_en
gdp = pd.read_csv("gdp_qtr_real_supply_sub.csv", parse_dates=["date"])  # series,date,sector,value

print("✅ Files loaded:")
print(f" - Tourism sectors: {len(sectors)} rows")
print(f" - GDP records: {len(gdp)} rows\n")

# =========================================================
# 2️⃣ Keep only tourism-related sectors
# =========================================================
df = gdp.merge(sectors, on="sector", how="inner")

# Extract year from date
df["year"] = df["date"].dt.year

# Keep 2020–2023 only
df = df.query("2020 <= year <= 2023").copy()

# Drop rows without valid GDP values
df["value"] = pd.to_numeric(df["value"], errors="coerce")
df = df.dropna(subset=["value"])

print(f"✅ Filtered dataset: {df['sector'].nunique()} sectors × {df['year'].nunique()} years\n")

# =========================================================
# 3️⃣ Aggregate to one row per sector–year (sum of values)
# =========================================================
df_agg = (
    df.groupby(["year", "sector", "desc_en"], as_index=False)
      .agg({"value": "sum"})
)

# =========================================================
# 4️⃣ Compute ranks (1 = highest)
# =========================================================
df_agg["rank"] = (
    df_agg.groupby("year")["value"]
          .rank(ascending=False, method="dense")
          .astype(int)
)

# Sort neatly for readability
df_agg = df_agg.sort_values(["year", "rank"])

# =========================================================
# 5️⃣ Save clean version for Vega-Lite
# =========================================================
output_path = "tourism_sector_ranks_clean.csv"
df_agg.to_csv(output_path, index=False, encoding="utf-8-sig")

print(f"✅ Saved {output_path}")
print(df_agg.head(12).to_string(index=False))

import pandas as pd

df = pd.read_csv("tourism_sector_ranks_clean.csv")
pivot = df.pivot(index="desc_en", columns="year", values="value").reset_index()
pivot["change"] = pivot[2023] - pivot[2020]
pivot.to_csv("tourism_sector_change.csv", index=False)
