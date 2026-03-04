"""
ieso_data_prep.py
-----------------
Loads, cleans, and merges the two IESO datasets:
  - datasets/PUB_IntertieScheduleFlowYear_2025.csv
  - datasets/PUB_Demand_2025.csv

Run from the repo root:
    python code/ieso_data_prep.py

Returns a merged hourly DataFrame saved to:
    datasets/merged_2025.csv
"""

import pandas as pd
import os

# ── Paths (relative to repo root) ──────────────────────────────────────────
INTERTIE_PATH = "datasets/PUB_IntertieScheduleFlowYear_2025.csv"
DEMAND_PATH   = "datasets/PUB_Demand_2025.csv"
OUTPUT_PATH   = "datasets/merged_2025.csv"


def load_intertie(path: str) -> pd.DataFrame:
    """
    Load the Yearly Intertie Schedule and Flow Report.
    The file has 3 comment rows, 1 region-name row, then the column header row.
    The last 3 columns (Imp.14, Exp.14, Flow.14) represent Ontario totals.
    """
    df = pd.read_csv(path, skiprows=4)

    # Rename total columns for clarity
    df = df.rename(columns={
        "Imp.14":  "Total_Imp",
        "Exp.14":  "Total_Exp",
        "Flow.14": "Total_Flow",
    })

    # Parse date and add time features
    df["Date"]       = pd.to_datetime(df["Date"])
    df["Hour"]       = df["Hour"].astype(int)
    df["Month"]      = df["Date"].dt.month
    df["Month_Name"] = df["Date"].dt.strftime("%b")
    df["Week"]       = df["Date"].dt.isocalendar().week.astype(int)
    df["DayOfWeek"]  = df["Date"].dt.day_name()

    # Net export: positive = Ontario exporting, negative = Ontario importing
    df["Net_Export"] = df["Total_Exp"] - df["Total_Imp"]

    keep = ["Date", "Hour", "Month", "Month_Name", "Week", "DayOfWeek",
            "Total_Imp", "Total_Exp", "Total_Flow", "Net_Export"]

    return df[keep]


if __name__ == "__main__":
    intertie = load_intertie(INTERTIE_PATH)

    print(f"\nMerged dataset saved to: {OUTPUT_PATH}")