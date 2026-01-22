# src/preprocess.py
import argparse, json, os
import pandas as pd
from preprocess_pipeline import preprocessing_steps

def basic_quality_checks(df: pd.DataFrame):
    assert len(df) > 0, "Empty DataFrame after preprocessing"

    # Example rule: no nulls in key columns
    for col in ["id"]:
        if col in df.columns:
            assert df[col].notna().all(), f"Nulls found in required column: {col}"

def main(cfg_path: str):
    import yaml
    with open(cfg_path, "r") as f:
        cfg = yaml.safe_load(f)

    raw_path = cfg["raw_path"]           # e.g., data/raw/sample.csv
    out_path = cfg["processed_path"]     # e.g., data/processed/clean.csv

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    df = pd.read_csv(raw_path)
    df = preprocessing_steps(df.copy(), raw_path, out_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    main(args.config)