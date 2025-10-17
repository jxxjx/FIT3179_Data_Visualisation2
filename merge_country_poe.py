#!/usr/bin/env python3
# build_tourism_data.py
# Usage:
#   python build_tourism_data.py

import pandas as pd
from pathlib import Path

# ========= 1) File paths =========
ARRIVALS_FILE = "foreign_arrivals.csv"   # poe,country,date,arrivals[,arrivals_male,arrivals_female]
POE_FILE      = "poe.csv"                # poe,state,lat,long,type,abb,iso
COUNTRY_META  = "countries.csv"          # optional: Continent_Name,Three_Letter_Country_Code,Country_Name,...
OUT_PREFIX    = "tourism_flow"

# ========= 2) Fallback region map (used only if COUNTRY_META missing or unmatched) =========
REGION_MAP = {
    # ASEAN
    "MYS": "ASEAN", "SGP": "ASEAN", "IDN": "ASEAN", "THA": "ASEAN",
    "VNM": "ASEAN", "PHL": "ASEAN", "MMR": "ASEAN", "KHM": "ASEAN",
    "BRN": "ASEAN", "LAO": "ASEAN",
    # East Asia
    "CHN": "East Asia", "JPN": "East Asia", "KOR": "East Asia",
    "TWN": "East Asia", "HKG": "East Asia", "MAC": "East Asia",
    # South Asia
    "IND": "South Asia", "PAK": "South Asia", "BGD": "South Asia",
    "LKA": "South Asia", "NPL": "South Asia", "AFG": "South Asia",
    # Middle East
    "SAU": "Middle East", "ARE": "Middle East", "QAT": "Middle East",
    "KWT": "Middle East", "BHR": "Middle East", "OMN": "Middle East",
    "IRN": "Middle East", "TUR": "Middle East",
    # Europe (core + additions)
    "GBR": "Europe", "FRA": "Europe", "DEU": "Europe", "ITA": "Europe",
    "ESP": "Europe", "NLD": "Europe", "SWE": "Europe", "NOR": "Europe",
    "FIN": "Europe", "RUS": "Europe", "POL": "Europe", "CHE": "Europe",
    "BEL": "Europe", "AUT": "Europe", "DNK": "Europe", "CZE": "Europe",
    "PRT": "Europe", "GRC": "Europe", "IRL": "Europe", "HUN": "Europe",
    # Oceania
    "AUS": "Oceania", "NZL": "Oceania", "FJI": "Oceania", "PNG": "Oceania",
    # Americas (+ additions)
    "USA": "Americas", "CAN": "Americas", "BRA": "Americas", "MEX": "Americas",
    "ARG": "Americas", "CHL": "Americas", "COL": "Americas", "PER": "Americas",
    "VEN": "Americas", "CUB": "Americas", "PAN": "Americas", "URY": "Americas",
    "JAM": "Americas", "DOM": "Americas",
    # Africa
    "ZAF": "Africa", "EGY": "Africa", "KEN": "Africa", "NGA": "Africa",
    "MAR": "Africa", "ETH": "Africa", "TUN": "Africa", "TZA": "Africa", "GHA": "Africa",
    # Central Asia
    "KAZ": "Central Asia", "UZB": "Central Asia", "TKM": "Central Asia",
    "KGZ": "Central Asia", "TJK": "Central Asia",
    # Pacific islands
    "WSM": "Oceania", "TON": "Oceania", "SLB": "Oceania"
}

# ========= 3) Load data =========
for p in (ARRIVALS_FILE, POE_FILE):
    if not Path(p).exists():
        raise FileNotFoundError(f"Missing file: {p}")

arrivals = pd.read_csv(ARRIVALS_FILE)
poe = pd.read_csv(POE_FILE)

# normalize colnames
arrivals.columns = arrivals.columns.str.strip().str.lower()
poe.columns      = poe.columns.str.strip().str.lower()

# ========= 4) Parse dates & year =========
arrivals["date"] = pd.to_datetime(arrivals["date"], errors="coerce")
bad = arrivals[arrivals["date"].isna()]
if not bad.empty:
    print("⚠️ Unparseable dates dropped (showing up to 5):")
    print(bad[["poe", "country", "date"]].head())
arrivals = arrivals.dropna(subset=["date"])
arrivals["year"] = arrivals["date"].dt.year

# ensure numeric
for c in ("arrivals", "arrivals_male", "arrivals_female"):
    if c in arrivals.columns:
        arrivals[c] = pd.to_numeric(arrivals[c], errors="coerce").fillna(0)

# ========= 5) Merge arrivals + POE =========
merged = arrivals.merge(poe, on="poe", how="left", indicator=True)
if (merged["_merge"] != "both").any():
    print("⚠️ POEs not found in poe.csv:", sorted(merged.loc[merged["_merge"]!="both","poe"].dropna().unique())[:20])
merged = merged.drop(columns="_merge")

# ==========================================================
# 6️⃣ Attach country metadata (always prefer metadata name if available)
# ==========================================================
country_full = None
region_from_meta = None

if Path(COUNTRY_META).exists():
    meta = pd.read_csv(COUNTRY_META)
    meta.columns = meta.columns.str.strip().str.lower()

    # rename flexible column variants
    ren = {}
    for want, candidates in {
        "continent_name": ["continent_name","continent","continentname"],
        "three_letter_country_code": ["three_letter_country_code","alpha_3_code","iso3","three_lett"],
        "country_name": ["country_name","name","country_n"]
    }.items():
        for c in candidates:
            if c in meta.columns:
                ren[c] = want
                break
    meta = meta.rename(columns=ren)

    if {"continent_name","three_letter_country_code","country_name"}.issubset(meta.columns):
        meta["three_letter_country_code"] = meta["three_letter_country_code"].str.upper().str.strip()
        merged["country"] = merged["country"].str.upper().str.strip()

        # 💡 Clean country names: take only text before comma
        meta["country_name"] = meta["country_name"].astype(str).str.split(",").str[0].str.strip()

        # merge metadata
        merged = merged.merge(
            meta[["three_letter_country_code","country_name","continent_name"]],
            left_on="country",
            right_on="three_letter_country_code",
            how="left"
        )

        country_full = "country_name"
        region_from_meta = "continent_name"

        # report missing
        miss = merged[merged[country_full].isna()]["country"].dropna().unique().tolist()
        if miss:
            print(f"ℹ️ Some countries not found in {COUNTRY_META}: {miss[:20]}{' …' if len(miss)>20 else ''}")
    else:
        print(f"⚠️ {COUNTRY_META} found but missing required columns — skipping metadata join.")
else:
    print("ℹ️ No countries.csv found — using REGION_MAP fallback.")

# ==========================================================
# 7️⃣ Region derivation & naming preference
# ==========================================================
merged["country"] = merged["country"].str.upper().str.strip()

# --- Region: prefer metadata continent; else fallback; else Other
if region_from_meta and region_from_meta in merged.columns:
    merged["region"] = merged[region_from_meta]
else:
    merged["region"] = merged["country"].map(REGION_MAP)
merged["region"] = merged["region"].fillna("Other")

# --- Country label: always prefer metadata name (already cleaned)
if country_full and country_full in merged.columns:
    merged["country_label"] = merged[country_full].fillna(merged["country"])
else:
    merged["country_label"] = merged["country"]


# ========= 8) Aggregations =========
# full detail by year/region/country/poe/state
agg = (
    merged.groupby(["year", "region", "country", "country_label", "poe", "state"], as_index=False)
          .agg({
               "arrivals":"sum",
               **({"arrivals_male":"sum"} if "arrivals_male" in merged.columns else {}),
               **({"arrivals_female":"sum"} if "arrivals_female" in merged.columns else {})
          })
          .rename(columns={
              "arrivals":"total_arrivals",
              "arrivals_male":"male_arrivals",
              "arrivals_female":"female_arrivals"
          })
          .sort_values(["year","region","total_arrivals"], ascending=[True,True,False])
)

# overall totals for (country,poe,state)
agg_country = (
    agg.groupby(["country","country_label","poe","state"], as_index=False)["total_arrivals"].sum()
)

# true region summary (just region totals)
agg_region = (
    agg.groupby(["region"], as_index=False)["total_arrivals"].sum()
)

# ========= 9) Save =========
Path("output").mkdir(exist_ok=True)

f_detail  = f"output/{OUT_PREFIX}_region_country_poe_state_year.csv"
f_country = f"output/{OUT_PREFIX}_country_poe_state.csv"
f_region  = f"output/{OUT_PREFIX}_region_summary.csv"

agg.to_csv(f_detail, index=False)
agg_country.to_csv(f_country, index=False)
agg_region.to_csv(f_region, index=False)

print(f"✅ Saved: {f_detail}")
print(f"✅ Saved: {f_country}")
print(f"✅ Saved: {f_region}")

# ========= 10) Preview =========
print("\nPreview (first 8 detail rows):")
print(agg.head(8).to_string(index=False))

print("\n🎯 Ready for viz:")
print(f"  - Donut: load {f_region} (fields: region,total_arrivals)")
print(f"  - Linked Donut + Top-10 Countries: load {f_detail} and use country_label in the bar chart")
print("  - Map: use lat/long from poe.csv joined in the detail file")
