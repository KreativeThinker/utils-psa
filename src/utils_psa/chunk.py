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


def per_chunk_analysis(input_base_path: Path, output_base_dir: Path) -> Path:
    """For each chunk index (chunk_00, chunk_01, ...), computes mean amplitude
    per frequency across all tests for a given animal and sleep state. Outputs
    one file per chunk:
    output_base_dir/CHUNKS/{REM|NREM}/{ANIMAL}/chunk_XX.csv.

    The final output has:
        Frequency, mean_amp_bl1, mean_amp_bl2, mean_amp_test1, ...

    Args:
        input_base_path (Path): Base directory containing input chunked files.
        output_base_dir (Path): Where to store aggregated output.

    Returns:
        Path: Base output path (i.e., output_base_dir / CHUNKS).
    """
    if not input_base_path.exists():
        raise FileNotFoundError(
            f"Input path does not exist: {input_base_path}"
        )

    chunk_output_base = output_base_dir / "chunks"
    chunk_output_base.mkdir(parents=True, exist_ok=True)

    for animal_dir in input_base_path.iterdir():
        if not animal_dir.is_dir():
            continue
        animal = animal_dir.name

        for sleep_dir in animal_dir.iterdir():
            if not sleep_dir.is_dir():
                continue
            sleep_state = sleep_dir.name.upper()  # REM/NREM

            chunked_path = sleep_dir / "chunked"
            if not chunked_path.exists():
                continue

            # Map: chunk_index -> { test_name -> DataFrame of that chunk }
            chunks = {}

            for test_dir in chunked_path.iterdir():
                if not test_dir.is_dir():
                    continue
                test_id = test_dir.name.lower()

                for chunk_file in test_dir.glob("chunk_*.csv"):
                    chunk_index = chunk_file.stem  # e.g., "chunk_00"

                    df = pd.read_csv(chunk_file)

                    # Drop metadata columns
                    df = df.drop(
                        columns=[
                            col
                            for col in df.columns
                            if col in ["EpochNo", "Stage", "Time"]
                            or col.startswith("Unnamed")
                        ],
                        errors="ignore",
                    )
                    if df.empty:
                        continue

                    # Calculate mean per frequency
                    means = df.mean(axis=0)

                    # Construct single-column dataframe for this test
                    result_df = pd.DataFrame(
                        {
                            "Frequency": means.index,
                            f"mean_amp_{test_id}": means.values,
                        }
                    )

                    # Store in chunks dict
                    if chunk_index not in chunks:
                        chunks[chunk_index] = {}
                    chunks[chunk_index][test_id] = result_df

            # Write output per chunk
            for chunk_index, test_dfs in chunks.items():
                # Merge all test DataFrames on Frequency
                merged_df = None
                for df in test_dfs.values():
                    if merged_df is None:
                        merged_df = df
                    else:
                        merged_df = merged_df.merge(
                            df, on="Frequency", how="outer"
                        )

                # Sort frequencies numerically
                merged_df["FrequencyHz"] = (
                    merged_df["Frequency"]
                    .str.replace("Hz", "", regex=False)
                    .astype(float)
                )
                merged_df = merged_df.sort_values("FrequencyHz").drop(
                    columns="FrequencyHz"
                )

                # Output path: CHUNKS/{REM|NREM}/{ANIMAL}/chunk_XX.csv
                output_chunk_dir = chunk_output_base / sleep_state / animal
                output_chunk_dir.mkdir(parents=True, exist_ok=True)
                output_path = output_chunk_dir / f"{chunk_index}.csv"
                merged_df.to_csv(output_path, index=False)
                print(f"[✓] Wrote {output_path}")

    return chunk_output_base
