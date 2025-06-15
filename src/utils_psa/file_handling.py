from pathlib import Path

import pandas as pd


def find_traces_cfft_files(raw_data_base_dir: Path) -> list[Path]:
    """Finds all 'Traces_cFFT.csv' files recursively within the specified raw
    data base directory.

    Args:
        raw_data_base_dir (Path): The root directory where raw animal data
                                  (e.g., 'data/RAT1/') is located.

    Returns:
        list[Path]: A list of Path objects for all found 'Traces_cFFT.csv' files.
    """
    return list(raw_data_base_dir.rglob("*_cFFT.csv"))


def clean_file(input_filepath: Path, output_base_dir: Path) -> Path | None:
    """Removes the first 20 lines (metadata) from a Traces_cFFT.csv file and
    saves the cleaned data to the data/input/{animal}/{baseline|test}/
    directory.

    Args:
        input_filepath (Path): The full path to the raw Traces_cFFT.csv file.
        output_base_dir (Path): The root output directory (e.g., './data/processed/').

    Returns:
        Path | None: The path to the cleaned CSV file if successful, otherwise None.
    """
    try:
        # Determine animal and session type (baseline/test) from the input file path.
        # Example input_filepath: /path/to/data/RAT1/Ananya_rat1_BL1/Traces_cFFT.csv
        parts = input_filepath.parts

        # Find the 'data' part in the path to correctly extract animal and session.
        try:
            data_idx = parts.index("data")
        except ValueError:
            # Fallback if 'data' is not directly in path; assume specific structure
            # This is less robust and relies on the user providing paths consistent
            # with the expected 'data/animal/session' structure.
            print(
                f"Warning: 'data' not found in path {input_filepath}. Inferring paths based on parent directories."
            )
            # Assume structure like {animal_dir}/{session_dir}/{filename}
            session_dir = input_filepath.parent
            animal_dir = session_dir.parent
            animal = animal_dir.name
            session_name = session_dir.name
        else:
            if len(parts) > data_idx + 2:
                animal = parts[data_idx + 1]
                session_name = parts[data_idx + 2]
            else:
                raise ValueError(
                    f"Unexpected path structure: {input_filepath}. Expected '/data/{{animal}}/{{session_name}}/...'"
                )

        # Define the output directory for the cleaned file: {output_base_dir}/input/{animal}/{session_type}/
        output_dir = output_base_dir / "input" / animal
        output_dir.mkdir(parents=True, exist_ok=True)

        # Create the output filename
        output_filepath = output_dir / f"{session_name}_cleaned.csv"
        if output_filepath.exists():
            print(f"Skipping '{output_filepath}' as it already exists.")
            return output_filepath
        # Read the file, skipping the first 20 lines
        # Using encoding='latin1' as often biological data CSVs use this.
        df = pd.read_csv(
            input_filepath, skiprows=20, encoding="latin1", low_memory=False
        )

        df.to_csv(output_filepath, index=False)
        print(
            f"Cleaned '{input_filepath.name}' and saved to '{output_filepath}'"
        )
        return output_filepath

    except pd.errors.EmptyDataError:
        print(
            f"Error: File '{input_filepath}' is empty or contains no data after skipping 20 lines."
        )
        return None
    except Exception as e:
        print(f"Error cleaning file '{input_filepath}': {e}")
        return None


def create_output_directories(output_base_dir: Path, animals: list[str]):
    """Creates the necessary initial output directory structure based on the
    identified animals.

    The structure created is:
    - {output_base_dir}/input/
    - {output_base_dir}/{animal}/{session}/rem/original/
    - {output_base_dir}/{animal}/{session}/nrem/original/
    - {output_base_dir}/{animal}/{session}/rem/chunked/
    - {output_base_dir}/{animal}/{session}/nrem/chunked/

    Args:
        output_base_dir (Path): The root output directory.
        animals (list[str]): A list of animal names (e.g., ['RAT1', 'RAT2']).
    """
    input_dir = output_base_dir / "input"
    input_dir.mkdir(parents=True, exist_ok=True)

    for animal in animals:
        for state in ["rem", "nrem"]:
            # Directories for original (preprocessed) split data
            (output_base_dir / animal / state / "original").mkdir(
                parents=True, exist_ok=True
            )
            # Directories for chunked and analyzed data
            (output_base_dir / animal / state / "chunked").mkdir(
                parents=True, exist_ok=True
            )
    print(
        f"Output directories created under '{output_base_dir}' for animals: {', '.join(animals)}"
    )
