# CollabSense Data Gathering

Data pipeline used to scrape, process, and clean data from open-source GitHub repositories for the CollabSense research project.

Uses an asynchronous scraper with multi-token rate limit handling, a processing engine that generates specific datasets to be used by the clarityloop platform.

## Project Structure

```text
collab-sense-data-gathering/
├── src/
│   ├── scraper.py     # Async GitHub scraper with token rotation
│   ├── processor.py   # Filtering logic & CSV generation (Standard & LTC)
│   ├── cleaner.py     # Synthetic data generation
│   ├── pipeline.py    # Main orchestrator for the workflow
│   └── config.py      # Configuration settings (Repo, Thresholds, Paths)
├── data/              # Output directory for all runs
├── .env               # API Secrets (Not committed)
└── requirements.txt   # Python dependencies
```

## Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/clarityloop/collabsense.git
    cd collabsense
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configure Environment Variables:**
    Create a `.env` file in the root directory. You must add at least one GitHub Personal Access Token (PAT).
    
    To bypass GitHub's rate limit (5,000 requests/hour), add multiple tokens. The scraper automatically manages and rotates them.

    ```text
    # .env file
    GITHUB_TOKEN=ghp_yourPrimaryTokenHere...
    
    # Optional: Add more tokens for larger scrapes
    GITHUB_TOKEN_1=ghp_secondaryTokenHere...
    GITHUB_TOKEN_2=ghp_tertiaryTokenHere...
    ```

4.  **Configure Target & Thresholds:**
    Open `src/config.py` to set the target repository and adjust filtering logic:
    *   `OWNER` / `REPO`: The target GitHub repository.
    *   `FILTER_...`: Thresholds for the Standard Dataset.
    *   `LTC_...`: Thresholds for the Long-Term Contributor Dataset.

## Usage

The project is controlled via the **Pipeline Orchestrator** (`src/pipeline.py`), which handles directory management and workflow execution.

### 1. Full "Start-to-Finish" Run
Scrapes data, creates a timestamped folder, processes both datasets, and cleans/anonymizes the results.
```bash
python -m src.pipeline
```
*Output:* `data/{OWNER}-{REPO}_SCRAPE_{TIMESTAMP}/`

### 2. Scraping Only
Fetches raw data and saves it to a new folder without processing.
```bash
python -m src.pipeline --scrape
```

### 3. Processing Existing Data (Re-Run Experiments)
If you already have a raw scrape file (`_FINAL.csv`) and want to re-run filters or generate new datasets without re-scraping:
```bash
python -m src.pipeline --process --clean --input-file data/path/to/existing_FINAL.csv
```
*Output:* Creates a **new** folder `data/{OWNER}-{REPO}_PROCESS_{TIMESTAMP}/` containing the new results.

### 4. Cleaning Only
To re-run the anonymization/cleaning logic on an existing folder:
```bash
python -m src.pipeline --clean --input-dir data/path/to/processed_folder
```

### Pipeline Arguments
| Argument | Description |
| :--- | :--- |
| `--mode` | Which dataset logic to run: `standard`, `ltc`, or `all` (default). |
| `--input-file` | Path to a raw CSV file (Required for `--process` only). |
| `--input-dir` | Path to a folder (Required for `--clean` only). |

## Output Files
The pipeline generates 5 CSV files formatted for ClarityLoop ingestion:
*   `users.csv`: Anonymized user profiles.
*   `workspaces.csv`: Project metadata.
*   `workspace_members.csv`: Workspace membership list.
*   `contexts.csv`: Pull Requests and Issues (The "Cases").
*   `context_comments.csv`: Feedback and discussion linked to contexts.
