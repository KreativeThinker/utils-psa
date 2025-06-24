from pathlib import Path

import pandas as pd


def normalize_data(
    input_filepath: Path,
    output_base_dir: Path,
) -> Path:
    """Normalize the input CSV file.

    1. Read the CSV into a pandas DataFrame.
    2. Normalize each column by dividing it by its mean.
    3. Normalize each row by dividing it by the sum of its values.
    4. Compute 'BL' as the average of the first two normalized columns.
    5. Save the output to a structured path based on metadata extracted from the path.

    Args:
        input_filepath: Path to the input CSV file.
        output_base_dir: Base directory to write normalized output.
        baseline_session_type: Used to infer condition (default: 'baseline').

    Returns:
        Path to the saved normalized file.
    """
    df = pd.read_csv(input_filepath, encoding="latin1", low_memory=False)
    # Extract and clean frequency column (remove 'Hz', convert to float if possible)
    freq = (
        df.iloc[:, 0]
        .astype(str)
        .str.replace("Hz", "", regex=False)
        .str.strip()
    )

    # Try to convert to float where possible
    freq = pd.to_numeric(freq, errors="coerce")

    # Drop the frequency column from df
    df = df.iloc[:, 1:].reset_index(drop=True)

    # Step 1: Column-wise normalization (divide each column by its mean)
    df = df / df.mean()

    # Step 2: Row-wise normalization (divide each row by its sum)
    df = df.div(df.sum(axis=1), axis=0)

    # Step 3: Compute BL as average of first two columns
    df["BL"] = df.iloc[:, 0:2].mean(axis=1)
    df.insert(0, "Frequency", freq)

    # Infer metadata from path: assumes structure like .../{animal}/{chunk}/{file}
    parts = input_filepath.parts
    try:
        chunk = parts[-2]
        animal = parts[-3]
    except IndexError:
        raise ValueError(f"Unexpected path structure in: {input_filepath}")

    # Prepare output path
    output_path = output_base_dir / "normalized" / animal / chunk
    output_path.mkdir(parents=True, exist_ok=True)

    output_file = output_path / input_filepath.name
    df.to_csv(output_file, index=False)

    return output_file
