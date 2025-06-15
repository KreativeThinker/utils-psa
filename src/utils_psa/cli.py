from pathlib import Path

import typer
from typing_extensions import Annotated

from . import chunk, file_handling, normalize, preprocess

app = typer.Typer(
    help="Spectral Power Analysis tool for rat EEG data during REM and NREM sleep."
)


@app.command()
def run_analysis(
    raw_data_dir: Annotated[
        Path,
        typer.Option(
            help="Path to the top-level raw EEG data directory (e.g., './data/')."
            "This directory should contain animal subdirectories like 'RAT1/', 'RAT2/', etc."
        ),
    ],
    output_data_dir: Annotated[
        Path,
        typer.Option(
            help="Path to the output directory where all processed data will be saved (e.g., './data/processed/')."
        ),
    ],
    chunk_size: Annotated[
        int,
        typer.Option(
            help="Number of epochs (rows) per chunk for analysis.", min=1
        ),
    ] = 100,  # Default chunk size if not specified
    baseline_type: Annotated[
        str,
        typer.Option(
            help="The session type to use as the baseline for normalization (e.g., 'baseline' for 'BL1' or 'BL2').",
        ),
    ] = "BL1",  # Default baseline type for normalization
):
    """
    Performs the full spectral chunk-based analysis workflow:
    1. Cleans raw Traces_cFFT.csv files (removes metadata).
    2. Preprocesses cleaned data (transposes, filters Wake, splits into REM/NREM).
    3. Chunks REM/NREM data by time.
    4. Performs per-chunk analysis (averages frequencies) and combines results.
    5. Normalizes combined chunk data relative to a specified baseline.
    """
    if not raw_data_dir.is_dir():
        typer.echo(
            f"Error: Raw data directory '{raw_data_dir}' does not exist or is not a directory."
        )
        raise typer.Exit(code=1)

    # Ensure output_data_dir exists (it will be created if it doesn't)
    output_data_dir.mkdir(parents=True, exist_ok=True)
    typer.echo("Starting analysis...")
    typer.echo(f"  Raw data source: '{raw_data_dir}'")
    typer.echo(f"  Output destination: '{output_data_dir}'")
    typer.echo(f"  Chunk size set to: {chunk_size} epochs")
    typer.echo(f"  Normalization baseline type: '{baseline_type}'")

    # --- Step 1: Clean files ---
    typer.echo("\n--- Step 1: Cleaning raw Traces_cFFT.csv files ---")
    # Find all Traces_cFFT.csv files within the raw_data_dir structure
    traces_cfft_files = file_handling.find_traces_cfft_files(raw_data_dir)

    if not traces_cfft_files:
        typer.echo(
            "No 'Traces_cFFT.csv' files found in the specified raw data directory structure. Please check the path and file names."
        )
        raise typer.Exit(code=1)

    cleaned_input_dir = output_data_dir / "input"
    cleaned_input_dir.mkdir(
        parents=True, exist_ok=True
    )  # Ensure this base input directory exists

    # Discover unique animal names by iterating through parent directories of cFFT files
    animals = sorted(
        list(set([f.parent.parent.name for f in traces_cfft_files]))
    )
    if not animals:
        typer.echo(
            "Could not determine animal directories from raw data paths. Exiting."
        )
        raise typer.Exit(code=1)

    file_handling.create_output_directories(
        output_data_dir, animals
    )  # Create main output structure

    cleaned_files_count = 0
    for file_path in traces_cfft_files:
        cleaned_file_path = file_handling.clean_file(
            file_path, output_data_dir
        )  # Pass output_data_dir as base
        if cleaned_file_path:
            cleaned_files_count += 1

    if cleaned_files_count == 0:
        typer.echo(
            "No raw data files were cleaned successfully. Analysis cannot proceed. Exiting."
        )
        raise typer.Exit(code=1)

    # --- Step 2: Preprocess and split ---
    typer.echo(
        "\n--- Step 2: Preprocessing and splitting data into REM/NREM ---"
    )
    processed_rem_nrem_files = (
        []
    )  # This will store paths to the newly created REM/NREM files

    # Iterate through the newly created cleaned input files
    for animal in animals:
        for session_type_sub_dir in [
            (cleaned_input_dir / animal / "baseline"),
            (cleaned_input_dir / animal / "test"),
        ]:
            if session_type_sub_dir.is_dir():
                for cleaned_csv in session_type_sub_dir.glob("*.csv"):
                    if preprocess.preprocess_and_split(
                        cleaned_csv, output_data_dir
                    ):
                        # After preprocessing, the files are in {output_data_dir}/{animal}/{rem|nrem}/original/{baseline|test}/
                        for state in ["rem", "nrem"]:
                            original_files_path = (
                                output_data_dir
                                / animal
                                / state
                                / "original"
                                / session_type_sub_dir.name
                            )
                            if original_files_path.is_dir():
                                processed_rem_nrem_files.extend(
                                    list(original_files_path.glob("*.csv"))
                                )

    if not processed_rem_nrem_files:
        typer.echo(
            "No REM/NREM data files were processed and split successfully. Analysis cannot proceed. Exiting."
        )
        raise typer.Exit(code=1)
    typer.echo(
        f"Successfully processed and split {len(processed_rem_nrem_files)} REM/NREM files."
    )

    # --- Step 3: Chunk by time ---
    typer.echo("\n--- Step 3: Chunking data by time ---")
    chunked_files_count = 0
    for processed_file in processed_rem_nrem_files:
        if chunk.chunk_by_time(processed_file, output_data_dir, chunk_size):
            chunked_files_count += 1

    if chunked_files_count == 0:
        typer.echo(
            "No files were chunked successfully. Analysis cannot proceed. Exiting."
        )
        raise typer.Exit(code=1)
    typer.echo(f"Successfully created {chunked_files_count} chunks.")

    # --- Step 4: Per-chunk analysis and combine ---
    typer.echo("\n--- Step 4: Performing per-chunk analysis and combining ---")
    # Determine the maximum chunk number generated to iterate correctly
    max_chunk_num = -1
    for path in (output_data_dir).rglob("*_??.csv"):
        try:
            # Filenames are like 'Traces_cFFT_baseline_00.csv', 'Traces_cFFT_test_01.csv'
            # Extract the last two characters before .csv as chunk number
            chunk_str = path.stem[-2:]
            current_chunk_num = int(chunk_str)
            if current_chunk_num > max_chunk_num:
                max_chunk_num = current_chunk_num
        except (IndexError, ValueError):
            continue

    if max_chunk_num < 0:
        typer.echo(
            "No chunked files found to perform per-chunk analysis. Exiting."
        )
        raise typer.Exit(code=1)

    per_chunk_analysis_done_count = 0
    for animal in animals:
        for sleep_state in ["rem", "nrem"]:
            for i in range(max_chunk_num + 1):
                if chunk.per_chunk_analysis(
                    output_data_dir, animal, sleep_state, i
                ):
                    per_chunk_analysis_done_count += 1

    if per_chunk_analysis_done_count == 0:
        typer.echo(
            "Per-chunk analysis and combining failed for all relevant files. Analysis cannot proceed. Exiting."
        )
        raise typer.Exit(code=1)
    typer.echo(
        f"Successfully performed per-chunk analysis and combined for {per_chunk_analysis_done_count} datasets."
    )

    # --- Step 5: Normalize data ---
    typer.echo("\n--- Step 5: Normalizing data ---")
    normalized_files_count = 0
    for animal in animals:
        for sleep_state in ["rem", "nrem"]:
            for i in range(max_chunk_num + 1):
                raw_combined_file = (
                    output_data_dir
                    / animal
                    / sleep_state
                    / "chunked"
                    / f"chunk_{i:02d}_raw.csv"
                )
                if raw_combined_file.exists():
                    if normalize.normalize_data(
                        raw_combined_file, output_data_dir, baseline_type
                    ):
                        normalized_files_count += 1
                # else:
                # typer.echo(f"Skipping normalization for '{raw_combined_file}': File not found.")

    if normalized_files_count == 0:
        typer.echo(
            "Normalization failed for all relevant files. No normalized output generated. Exiting."
        )
        raise typer.Exit(code=1)
    typer.echo(f"Successfully normalized {normalized_files_count} datasets.")

    typer.echo("\n--- Spectral chunk-based analysis complete! ---")
    typer.echo(
        f"All processed and analyzed results are saved in: '{output_data_dir}'"
    )
