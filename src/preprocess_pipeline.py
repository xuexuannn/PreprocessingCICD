import argparse, json, os
import pandas as pd

def basic_quality_checks(df: pd.DataFrame):
    assert len(df) > 0, "Empty DataFrame after preprocessing"

    # Example rule: no nulls in key columns
    for col in ["id"]:
        if col in df.columns:
            assert df[col].notna().all(), f"Nulls found in required column: {col}"

def preprocessing_steps(df, raw_path, out_path):

    # 1) Trim spaces in string cols
    for c in df.select_dtypes(include=["object"]).columns:
        df[c] = df[c].astype(str).str.strip()

    # 2) Drop duplicates
    before = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    dropped = before - len(df)

    # 3) Simple type coercions
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Quality checks
    basic_quality_checks(df)

    # Save
    df.to_csv(out_path, index=False)

    # Emit a tiny metadata file that downstream steps can read
    meta = {
        "rows": int(len(df)),
        "cols": int(len(df.columns)),
        "duplicates_dropped": int(dropped),
        "source": raw_path,
        "output": out_path,
    }
    meta_path = out_path.replace(".csv", ".json")
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"Saved cleaned data to {out_path}")
    print(f"Saved metadata to {meta_path}")
    return df
