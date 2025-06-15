from pathlib import Path

import pandas as pd


def normalize_data(
    input_filepath: Path,
    output_base_dir: Path,
    baseline_session_type: str = "baseline",
) -> bool:
    """Normalizes the combined raw frequency data. First, it normalizes per- frequency
    (Z-score for each frequency column).

    Then, it normalizes the data with respect to the specified baseline session type (e.g., 'baseline').
    The baseline normalization is typically (value - BL_mean) / BL_std for each frequency.

    Saves to: data/output/{animal}/{rem|nrem}/chunked/chunk_{chunk_num}_norm.csv

    Args:
        input_filepath (Path): The path to the combined raw chunk CSV file (e.g., chunk_00_raw.csv).
        output_base_dir (Path): The root output directory.
        baseline_session_type (str): The session type to use as baseline (e.g., 'baseline').
                                     This refers to the 'SessionType' column in the input file.

    Returns:
        bool: True if normalization was successful, False otherwise.
    """
    try:
        df = pd.read_csv(input_filepath, encoding="latin1")

        # Determine animal, sleep state, and chunk number from the input filename
        # Example input_filepath: data/processed/RAT1/rem/chunked/chunk_00_raw.csv
        parts = input_filepath.parts
        # Find the 'chunked' directory to determine relative path components
        try:
            chunked_idx = parts.index("chunked")
        except ValueError:
            raise ValueError(
                f"Unexpected input path structure for normalization: '{input_filepath}'. 'chunked' directory not found."
            )

        animal = parts[chunked_idx - 2]  # e.g., 'RAT1'
        sleep_state = parts[chunked_idx - 1]  # e.g., 'rem' or 'nrem'
        # Extract chunk number from the filename stem (e.g., 'chunk_00_raw' -> 0)
        chunk_num_str = input_filepath.stem.split("_")[1]
        chunk_num = int(chunk_num_str)

        # Identify frequency columns (excluding metadata columns)
        freq_cols = [
            col
            for col in df.columns
            if col not in ["Animal", "SleepState", "SessionType", "ChunkNum"]
        ]

        if not freq_cols:
            print(
                f"Warning: No frequency columns found in '{input_filepath}'. Cannot normalize."
            )
            return False

        normalized_df = df.copy()

        # Step 1: Normalize per-frequency (Z-score for each frequency column across all sessions/animals in this chunk's combined file)
        # This normalizes the distribution of each frequency band's power values.
        for col in freq_cols:
            mean_val = df[col].mean()
            std_val = df[col].std()
            if std_val > 0:
                normalized_df[col] = (df[col] - mean_val) / std_val
            else:
                # If standard deviation is 0, all values are the same. Subtracting mean makes them 0.
                normalized_df[col] = df[col] - mean_val

        # Step 2: Normalize w.r.t the specified baseline session type (e.g., 'baseline')
        # This typically means (value - BL_mean) / BL_std, where BL_mean and BL_std are from the baseline data.

        baseline_data = normalized_df[
            normalized_df["SessionType"] == baseline_session_type
        ]

        if baseline_data.empty:
            print(
                f"Warning: No '{baseline_session_type}' data found for '{animal}' (chunk {chunk_num:02d}, {sleep_state}). Cannot perform BL1 normalization."
            )
            print(
                f"Saving only per-frequency normalized data for '{input_filepath}'."
            )
            # If no baseline data, save the per-frequency normalized data and indicate success.
            output_file_dir = (
                output_base_dir / animal / sleep_state / "chunked"
            )
            output_file_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_file_dir / f"chunk_{chunk_num:02d}_norm.csv"
            normalized_df.to_csv(output_file, index=False)
            return True  # Successfully normalized per-frequency

        # Calculate the mean and standard deviation for each frequency from the baseline data
        baseline_means = baseline_data[freq_cols].mean()
        baseline_stds = baseline_data[freq_cols].std()

        # Apply baseline normalization: (current_value - baseline_mean) / baseline_std
        for col in freq_cols:
            # Check if the standard deviation of the baseline for this frequency is non-zero
            if baseline_stds[col] > 0:
                normalized_df[col] = (
                    normalized_df[col] - baseline_means[col]
                ) / baseline_stds[col]
            else:
                # If std is zero, it means all baseline values for this frequency were identical.
                # Just subtract the mean. The result will be 0 if the current value matches the baseline.
                normalized_df[col] = normalized_df[col] - baseline_means[col]

        # Define the output path for the normalized file
        output_file_dir = output_base_dir / animal / sleep_state / "chunked"
        output_file_dir.mkdir(
            parents=True, exist_ok=True
        )  # Ensure directory exists
        output_file = output_file_dir / f"chunk_{chunk_num:02d}_norm.csv"

        normalized_df.to_csv(output_file, index=False)
        print(
            f"Normalized data for chunk {chunk_num:02d} ({animal} - {sleep_state}) and saved to '{output_file}'"
        )
        return True
    except Exception as e:
        print(f"Error normalizing file '{input_filepath}': {e}")
        return False
