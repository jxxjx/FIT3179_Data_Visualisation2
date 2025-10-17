import pandas as pd

# === 1. Load the data ===
arrivals = pd.read_csv("foreign_arrivals.csv")
gdp = pd.read_csv("gdp_qtr_real_supply_sub.csv")

# --- Make sure 'date' is parsed correctly ---
arrivals["date"] = pd.to_datetime(arrivals["date"], errors="coerce")
gdp["date"] = pd.to_datetime(gdp["date"], errors="coerce")

# === 1.1 Check for undefined / invalid dates ===
undefined_arrivals = arrivals[arrivals["date"].isna()]
undefined_gdp = gdp[gdp["date"].isna()]

if not undefined_arrivals.empty:
    print("⚠️ Undefined 'date' in foreign_arrivals.csv (cannot parse these rows):")
    print(undefined_arrivals)
else:
    print("✅ All arrival dates parsed correctly.")

if not undefined_gdp.empty:
    print("⚠️ Undefined 'date' in gdp_qtr_real_supply_sub.csv (cannot parse these rows):")
    print(undefined_gdp)
else:
    print("✅ All GDP dates parsed correctly.")

# === 2. Prepare Tourism Arrivals ===
arrivals_q = (
    arrivals.assign(
        Quarter=arrivals["date"].dt.to_period("Q").dt.start_time,
        Year=arrivals["date"].dt.year
    )
    .groupby(["Quarter", "Year"], as_index=False)
    .agg({"arrivals": "sum"})
    .rename(columns={"arrivals": "Tourist_Arrivals"})
)

arrivals_q["Tourism_YoY_%"] = arrivals_q["Tourist_Arrivals"].pct_change(4) * 100

# === 3. Prepare GDP ===
gdp_filtered = gdp.query("series == 'abs' and sector == 'p0'").copy()

gdp_q = (
    gdp_filtered.assign(
        Quarter=gdp_filtered["date"].dt.to_period("Q").dt.start_time,
        Year=gdp_filtered["date"].dt.year
    )
    .groupby(["Quarter", "Year"], as_index=False)
    .agg({"value": "sum"})
    .rename(columns={"value": "GDP"})
)

gdp_q["GDP_YoY_%"] = gdp_q["GDP"].pct_change(4) * 100

# === 4. Check for undefined years before merging ===
undefined_years_arrivals = arrivals_q[arrivals_q["Year"].isna()]
undefined_years_gdp = gdp_q[gdp_q["Year"].isna()]

if not undefined_years_arrivals.empty:
    print("\n⚠️ Rows in arrivals_q with undefined Year:")
    print(undefined_years_arrivals)

if not undefined_years_gdp.empty:
    print("\n⚠️ Rows in gdp_q with undefined Year:")
    print(undefined_years_gdp)

# === 5. Merge datasets by Quarter ===
merged = pd.merge(arrivals_q, gdp_q, on=["Quarter", "Year"], how="inner")

# === 6. Optional: format for Vega-Lite ===
merged_out = merged[["Quarter", "Year", "GDP", "Tourist_Arrivals"]].copy()

# === 7. Save result ===
merged_out.to_csv("tourism_gdp_yoy.csv", index=False)
print("\n✅ Merged file saved as tourism_gdp_yoy.csv")
print(merged_out.head(8))
