# 🧠 util-psa

Spectral chunk-based analysis of rat EEG data during REM and NREM sleep.

## Project Structure

```
./
├── src/
│   └── utils-psa/
│       ├── chunk.py                # Handle chuking by time
│       ├── cli.py                  # App entrypoint
│       ├── file_handling.py        # Handle files and folders
│       ├── __init__.py
│       ├── normalize.py            # Normalize data
│       └── preprocess.py           # Preprocess and data cleaning
├── main.py
├── pyproject.toml
├── README.md
├── requirements.txt
└── uv.lock

3 directories, 11 files
```

## 🧪 Workflow

1. **Extract `Traces_cFFT.csv`**
   From: `data/raw/{animal}/{baseline|test}/`

2. **Clean files**

   - Remove first 20 lines (metadata)
   - Save to: `data/input/{animal}/{baseline|test}/`

3. **Preprocess**

   - Transpose CSV
   - Sort by 2nd column (R, NR, W)
   - Drop rows with 'W'

4. **Split**

   - Save REM and NREM to:
     `data/output/{animal}/{rem|nrem}/original/{baseline|test}/`

5. **Chunk by time**

   - Save to:
     `data/output/{animal}/{rem|nrem}/chunked/{baseline|test}_{chunk_num}`

6. **Per-chunk analysis**

   - Average frequency across epochs
   - Combine across baseline/test into:
     `data/output/{animal}/{rem|nrem}/chunked/{chunk_num}_raw`

7. **Normalize**

   - Normalize per-frequency
   - Then normalize w.r.t BL1
   - Save to:
     `data/output/{animal}/{rem|nrem}/chunked/{chunk_num}_norm`
