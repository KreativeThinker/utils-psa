from pathlib import Path
from typing import List

import pandas as pd


def preprocess_and_split(
    input_filepath: Path, output_base_dir: Path
) -> List[Path]:
    """
    Transposes the cleaned CSV data, sorts by the 'Stage' column,
    drops rows classified as 'W' (Wake), and then splits the data
    into REM (R) and NREM (NR) sleep stages.
    The resulting DataFrames are saved to:
    data/output/{animal}/{rem|nrem}/original/{baseline|test}/

    Args:
        input_filepath (Path): The path to the cleaned input CSV file.
        output_base_dir (Path): The root output directory.

    Returns:
        bool:  Success of the preprocessing and splitting operation.
    """
    try:

        # Determine animal, session type (baseline/test),
        # and original filename stem
        parts = input_filepath.parts
        # Find the 'input' part to locate animal and session type
        try:
            input_idx = parts.index("input")
        except ValueError:
            raise ValueError(
                f"Unexpected input path structure: '{input_filepath}'."
                "'input' directory not found."
            )

        animal = parts[input_idx + 1]  # e.g., 'RAT1'
        session_type = parts[input_idx + 2].replace(
            "_cleaned.csv", ""
        )  # e.g., 'baseline' or 'test'

        output_rem_dir = output_base_dir / animal / "rem" / "original"
        output_nrem_dir = output_base_dir / animal / "nrem" / "original"
        output_rem_dir.mkdir(parents=True, exist_ok=True)
        output_nrem_dir.mkdir(parents=True, exist_ok=True)
        output_rem_file = output_rem_dir / f"{session_type}_rem.csv"
        output_nrem_file = output_nrem_dir / f"{session_type}_nrem.csv"

        if output_nrem_file.exists() and output_rem_file.exists():
            return [output_rem_file, output_nrem_file]

        df = pd.read_csv(input_filepath, encoding="latin1", low_memory=False)

        # Set the first row (which now contains original column names)
        # as the new DataFrame headers
        df.columns = df.iloc[0]
        # Drop the row that was just used for headers
        df = df[2:]
        # Reset index for cleaner DataFrame
        df = df.reset_index(drop=True)

        # Ensure 'Stage' column exists and is correctly typed
        if "Stage" not in df.columns:
            # Fallback if 'Stage' column is not explicitly named.
            # Based on sample, original 2nd column after transpose
            # would contain R, NR, W.
            if df.shape[1] > 1:
                # Assuming the second column in the transposed df
                # (index 1) is 'Stage' data
                df.rename(columns={df.columns[1]: "Stage"}, inplace=True)
            else:
                raise ValueError(
                    f"Could not find 'Stage' column or infer"
                    f"it from {input_filepath}."
                    "DataFrame columns: {df.columns.tolist()}"
                )

        df["Stage"] = df["Stage"].astype(str).str.strip()

        # Sort by 'Stage' column
        df_sorted = df.sort_values(by=["Stage", "Time"])

        # Drop rows with 'W' (Wake)
        df_filtered = df_sorted[df_sorted["Stage"] != "W"].copy()

        # Split into REM and NREM
        df_rem = df_filtered[df_filtered["Stage"] == "R"].copy()
        df_nrem = df_filtered[df_filtered["Stage"] == "NR"].copy()

        # Define output paths

        # Save to CSV
        df_rem.to_csv(output_rem_file, index=False)
        df_nrem.to_csv(output_nrem_file, index=False)

        print(
            f"Preprocessed '{input_filepath.name}'"
            "and split into REM/NREM files."
        )
        return [output_rem_file, output_nrem_file]
    except Exception as e:
        print(f"Error preprocessing file '{input_filepath}': {e}")
        return [input_filepath]
