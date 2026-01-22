import os
import pandas as pd
import yaml
import subprocess
import sys
# Included so that this test file can read src/preprocessing_pipeline.py
sys.path.append("src/") 
from preprocess_pipeline import preprocessing_steps

def test_preprocess_runs_successfully():
    with open("configs/preprocess.yaml", "r") as f:
        cfg = yaml.safe_load(f)

    output_path = cfg["processed_path"]
    # Remove existing output if present
    if os.path.exists(output_path):
        os.remove(output_path)

    # Run the script (same as CI)
    result = subprocess.run(
        ["python", "src/preprocess.py", "--config", "configs/preprocess.yaml"],
        capture_output=True,
        text=True
    )

    # Check it ran without crashing
    assert result.returncode == 0, f"Preprocess failed: {result.stderr}"

    # Check output file exists
    assert os.path.exists(output_path), "Cleaned CSV not created."


def test_processed_file_contents():
    with open("configs/preprocess.yaml", "r") as f:
        cfg = yaml.safe_load(f)

    output_path = cfg["processed_path"]
    df = pd.read_csv(output_path)
    df = preprocessing_steps(df.copy(), cfg["raw_path"] , output_path)

    # Basic checks: You may vary this to ensure that Git Actions can catch the errors.
    assert len(df) > 0, "Processed dataset is not empty."
    assert "id" in df.columns, "Missing required column: id"
    assert df["id"].notna().all(), "Nulls found in id column."
    assert df["id"].is_unique, "Duplicate IDs found."
