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


def per_chunk_analysis(input_filepath: Path, output_base_dir: Path) -> Path:
    """Average out frequency/amplitude over time for each chunk for each
    animal."""

    # check if input files exists

    # check if output file already exists

    # if not exists, read input file and perform following steps
    # for each corresponding chunk for all tests

    # drop columns [EpochNo, Stage, Time, Unnamed*]

    # Take mean of all columns

    # Take mean values and corresponding frequency values to make
    # a new dataframe of frequency, mean amplitude (columnar format)

    # append dataframe to output file with prepended test/baseline info
    # if baseline1 or bl1 then store as freq_bl1 and mean_amp_bl1 and so on

    # return path to output file

    return Path("asd")
