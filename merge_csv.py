import pandas as pd

# Load
arr = pd.read_csv("foreign_arrivals.csv", parse_dates=["date"])
gdp = pd.read_csv("gdp_qtr_real_supply_sub.csv", parse_dates=["date"])

# Tourism: aggregate to quarter and QoQ
arr_q = (arr.assign(qtr=arr["date"].dt.to_period("Q").dt.start_time)
            .groupby("qtr", as_index=False)["arrivals"].sum()
            .rename(columns={"arrivals":"arrivals_qtr"}))
arr_q["tourism_growth"] = arr_q["arrivals_qtr"].pct_change()*100

# GDP: filter and QoQ
gdp_q = (gdp.query("series=='abs' and sector=='p0'")
           .assign(qtr=gdp["date"].dt.to_period("Q").dt.start_time)
           .groupby("qtr", as_index=False)["value"].sum())  # if already 1 row/qtr, drop .sum()
gdp_q["gdp_growth"] = gdp_q["value"].pct_change()*100

# Join on quarter and limit to 2020–2023
merged = (pd.merge(arr_q[["qtr","tourism_growth"]], gdp_q[["qtr","gdp_growth"]], on="qtr", how="inner")
            .query("(qtr.dt.year >= 2020) & (qtr.dt.year <= 2023)"))

merged.to_csv("tourism_gdp_qoq.csv", index=False)
