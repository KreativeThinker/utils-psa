from pathlib import Path

import pandas as pd


def chunk_by_time(
    input_filepath: Path, output_base_dir: Path, chunk_size: int = 100
) -> bool:
    """Chunks the input DataFrame (REM or NREM data) by time (rows) into
    segments of 'chunk_size' epochs. Each chunk is saved as a separate CSV
    file.

    Saves to: data/output/{animal}/{rem|nrem}/chunked/{original_filename_stem}_{session_type}_{chunk_num}.csv

    Args:
        input_filepath (Path): The path to the preprocessed REM or NREM CSV file.
        output_base_dir (Path): The root output directory.
        chunk_size (int): The number of rows (epochs) per chunk.

    Returns:
        bool: True if chunking was successful, False otherwise.
    """
    try:
        df = pd.read_csv(input_filepath, encoding="latin1")

        # Determine animal, sleep state (rem/nrem), and session type (baseline/test)
        # Example input_filepath: data/processed/RAT1/rem/original/baseline/Traces_cFFT_rem.csv
        parts = input_filepath.parts
        # Find the 'original' directory to determine relative path components
        try:
            original_idx = parts.index("original")
        except ValueError:
            raise ValueError(
                f"Unexpected input path structure for chunking: '{input_filepath}'. 'original' directory not found."
            )

        animal = parts[original_idx - 2]  # e.g., 'RAT1'
        sleep_state = parts[original_idx - 1]  # e.g., 'rem' or 'nrem'
        session_type = parts[original_idx + 1]  # e.g., 'baseline' or 'test'
        original_filename_stem = input_filepath.stem.replace(
            "_rem", ""
        ).replace(
            "_nrem", ""
        )  # e.g., 'Traces_cFFT'

        num_chunks = (
            len(df) + chunk_size - 1
        ) // chunk_size  # Calculate total number of chunks

        for i in range(num_chunks):
            start_row = i * chunk_size
            end_row = min((i + 1) * chunk_size, len(df))
            chunk_df = df.iloc[start_row:end_row].copy()

            # Define the output directory for this chunk
            chunk_output_dir = (
                output_base_dir / animal / sleep_state / "chunked"
            )
            chunk_output_dir.mkdir(parents=True, exist_ok=True)

            # Define the filename for this chunk
            # Example: Traces_cFFT_baseline_00.csv
            chunk_filename = (
                f"{original_filename_stem}_{session_type}_{i:02d}.csv"
            )
            chunk_df.to_csv(chunk_output_dir / chunk_filename, index=False)
            print(
                f"Saved chunk {i:02d} of '{input_filepath.name}' to '{chunk_output_dir / chunk_filename}'"
            )
        return True
    except Exception as e:
        print(f"Error chunking file '{input_filepath}': {e}")
        return False


def per_chunk_analysis(
    output_base_dir: Path, animal: str, sleep_state: str, chunk_num: int
) -> bool:
    """Performs per-chunk analysis by averaging frequency across epochs for all
    session types (baseline/test) for a specific chunk number and animal, then
    combines these averages into a single raw file.

    Saves to: data/output/{animal}/{rem|nrem}/chunked/chunk_{chunk_num}_raw.csv

    Args:
        output_base_dir (Path): The root output directory.
        animal (str): The name of the animal (e.g., 'RAT1').
        sleep_state (str): The sleep state ('rem' or 'nrem').
        chunk_num (int): The current chunk number to analyze.

    Returns:
        bool: True if analysis and combining were successful, False otherwise.
    """
    all_chunks_data = []

    # Search for all chunk files belonging to this animal, sleep_state, and chunk_num
    # This will find files like 'Traces_cFFT_baseline_00.csv', 'Traces_cFFT_test_00.csv' etc.
    matching_files = list(
        (output_base_dir / animal / sleep_state / "chunked").glob(
            f"*_{chunk_num:02d}.csv"
        )
    )

    if not matching_files:
        # print(f"No matching chunk files found for animal: '{animal}', state: '{sleep_state}', chunk: {chunk_num:02d}.")
        return False  # No files to process

    for filepath in matching_files:
        try:
            df = pd.read_csv(filepath, encoding="latin1")

            # Identify frequency columns. These are numerical columns that are not 'EpochNo', 'Stage', 'Time'.
            # Based on the sample, 'EpochNo', 'Stage', 'Time' are likely the first three columns.
            non_freq_cols = ["EpochNo", "Stage", "Time"]
            # Filter out non-numeric columns and known metadata columns
            freq_cols = [
                col
                for col in df.columns
                if col not in non_freq_cols
                and pd.api.types.is_numeric_dtype(df[col])
            ]

            if not freq_cols:
                print(
                    f"Warning: No valid numeric frequency columns found in '{filepath}'. Skipping."
                )
                continue

            # Ensure frequency columns are numeric, coercing errors to NaN
            df_freq_numeric = df[freq_cols].apply(
                pd.to_numeric, errors="coerce"
            )
            # Drop any frequency columns that became entirely NaN (e.g., if they contained only text)
            df_freq_numeric = df_freq_numeric.dropna(axis=1, how="all")

            if df_freq_numeric.empty:
                print(
                    f"Warning: No valid frequency data remaining after cleaning for '{filepath}'. Skipping."
                )
                continue

            # Average frequency across epochs (rows) for the current chunk
            average_frequencies = (
                df_freq_numeric.mean().to_frame().T
            )  # .T makes it a single row DataFrame

            # Add metadata for identification
            # Filename example: Traces_cFFT_baseline_00.csv -> extract 'baseline' or 'test'
            filename_parts = filepath.stem.split("_")
            session_type = "unknown"
            if "baseline" in filename_parts:
                session_type = "baseline"
            elif "test" in filename_parts:
                session_type = "test"

            average_frequencies["Animal"] = animal
            average_frequencies["SleepState"] = (
                sleep_state  # Add sleep state for clarity in combined file
            )
            average_frequencies["SessionType"] = session_type
            average_frequencies["ChunkNum"] = chunk_num

            all_chunks_data.append(average_frequencies)

        except Exception as e:
            print(f"Error processing chunk file '{filepath}': {e}")

    if all_chunks_data:
        # Concatenate all averaged chunk dataframes
        combined_df = pd.concat(all_chunks_data, ignore_index=True)

        # Reorder columns to have metadata first for readability
        meta_cols = ["Animal", "SleepState", "SessionType", "ChunkNum"]
        final_cols = meta_cols + [
            col for col in combined_df.columns if col not in meta_cols
        ]
        combined_df = combined_df[final_cols]

        # Define output path for the combined raw analysis file
        output_file_dir = output_base_dir / animal / sleep_state / "chunked"
        output_file_dir.mkdir(
            parents=True, exist_ok=True
        )  # Ensure directory exists
        output_file = output_file_dir / f"chunk_{chunk_num:02d}_raw.csv"

        combined_df.to_csv(output_file, index=False)
        print(
            f"Combined raw analysis for chunk {chunk_num:02d} ({animal} - {sleep_state}) and saved to '{output_file}'"
        )
        return True
    else:
        # print(f"No data to combine for chunk {chunk_num:02d} ({animal} - {sleep_state}).")
        return False
