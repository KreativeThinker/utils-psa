from collections import defaultdict
from pathlib import Path

import pandas as pd


def combine_chunks(input_dir: Path, output_dir: Path) -> None:
    """Combine and average chunk files across animals for REM and NREM."""

    for sleep_state in ["REM", "NREM"]:
        state_input_dir = input_dir / sleep_state
        if not state_input_dir.exists():
            print(f"[!] Skipping missing state dir: {state_input_dir}")
            continue

        chunk_files = defaultdict(list)

        # Step 1: Collect chunk paths per chunk index
        for animal_dir in state_input_dir.iterdir():
            if not animal_dir.is_dir():
                continue
            for chunk_file in animal_dir.glob("chunk_*.csv"):
                chunk_files[chunk_file.name].append(chunk_file)

        # Step 2: Process each chunk index across animals
        for chunk_name, paths in chunk_files.items():
            merged_df = None
            col_counts = defaultdict(int)

            for path in paths:
                df = pd.read_csv(path)

                # Update counts only for numeric columns (exclude Frequency)
                for col in df.columns:
                    if col != "Frequency" and col in df:
                        col_counts[col] += 1

                if merged_df is None:
                    merged_df = df.copy()
                else:
                    merged_df = merged_df.merge(
                        df,
                        on="Frequency",
                        how="outer",
                        suffixes=(None, "_dup"),
                    )

            # Step 3: Average by grouping all columns with same base name
            averaged = pd.DataFrame()
            averaged["Frequency"] = merged_df["Frequency"]

            for col in col_counts:
                # Find all columns like 'T1', 'T1_dup', etc.
                relevant_cols = [
                    c for c in merged_df.columns if c.startswith(col)
                ]
                averaged[col] = merged_df[relevant_cols].mean(axis=1)

            # Output path: output_dir/{REM|NREM}/chunk_XX.csv
            output_chunk_dir = output_dir / "compiled" / sleep_state
            output_chunk_dir.mkdir(parents=True, exist_ok=True)

            output_path = output_chunk_dir / chunk_name
            averaged.to_csv(output_path, index=False)
            print(f"[✓] Wrote {output_path}")
