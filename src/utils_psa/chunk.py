from pathlib import Path

import pandas as pd
import typer


def parse_custom_time(time_str: str) -> float:
    """Parses a time string in HH:MM:SS:msms format into total seconds."""
    try:
        _, time_str = time_str.split(".", 1)
        hh, mm, ss = map(int, time_str.split(":"))
        return hh * 3600 + mm * 60 + ss
    except Exception as e:
        raise ValueError(f"Invalid time format: '{time_str}' — {e}")


def chunk_by_time(
    input_filepath: Path, output_base_dir: Path, chunk_size: int = 3600
) -> bool:
    """Splits a CSV based on 'Time' column (HH:MM:SS:cs format), into chunks
    each covering `chunk_size` seconds, and saves them as chunk_{num}.csv.

    Args:
        input_filepath (Path): Input CSV file path.
        output_base_dir (Path): Base directory for output.
        chunk_size (int): Time interval (seconds) per chunk.

    Returns:
        bool: True on success, False otherwise.
    """
    try:
        # Parse path to extract identifiers
        parts = input_filepath.parts
        try:
            original_idx = parts.index("original")
        except ValueError:
            raise ValueError(
                f"'original' folder not found in path: {input_filepath}"
            )

        animal = parts[original_idx - 2]
        sleep_state = parts[original_idx - 1]
        test = parts[original_idx + 1].replace(".csv", "")
        chunk_output_dir = (
            output_base_dir / animal / sleep_state / "chunked" / test
        )
        chunk_output_dir.mkdir(parents=True, exist_ok=True)

        chunk_files = list(chunk_output_dir.glob("chunk_*.csv"))
        if chunk_files:
            typer.echo(
                f"Chunked files already exists. Skipping for {input_filepath}"
            )
            return True

        df = pd.read_csv(input_filepath, encoding="latin1")

        if "Time" not in df.columns:
            raise ValueError(f"'Time' column missing in {input_filepath}")

        # Convert custom time format to total seconds
        df["TimeSeconds"] = df["Time"].apply(parse_custom_time)

        max_time = df["TimeSeconds"].iloc[-1]

        chunk_num = 0
        for start_time in range(0, max_time, chunk_size):
            end_time = start_time + chunk_size
            chunk_df = df[
                (df["TimeSeconds"] >= start_time)
                & (df["TimeSeconds"] < end_time)
            ].copy()

            if chunk_df.empty:
                continue

            chunk_output_dir = (
                output_base_dir / animal / sleep_state / "chunked" / test
            )
            chunk_output_dir.mkdir(parents=True, exist_ok=True)

            chunk_filename = f"chunk_{chunk_num:02d}.csv"
            chunk_df.drop(columns=["TimeSeconds"]).to_csv(
                chunk_output_dir / chunk_filename, index=False
            )

            print(
                f"Saved chunk {chunk_num:02d}: {start_time}s – {end_time}s → {chunk_filename}"
            )
            chunk_num += 1

        return True

    except Exception as e:
        print(f"Error chunking by time from '{input_filepath}': {e}")
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
