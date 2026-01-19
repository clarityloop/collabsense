# CollabSense Data Gathering

This repository contains the Python scripts used to mine and process collaboration data from GitHub repositories (e.g., Pull Requests, Issues, Comments) for the CollabSense research project.

## Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-org/collab-sense-data-gathering.git
    cd collab-sense-data-gathering
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configure Environment Variables:**
    Create a `.env` file in the root directory. You must add at least one GitHub Personal Access Token (PAT).
    
    To bypass GitHub's rate limit (5,000 requests/hour), you can add multiple tokens. The script automatically rotates them when one is exhausted.

    ```text
    # .env file
    GITHUB_TOKEN=ghp_yourPrimaryTokenHere...
    
    # Optional: Add more tokens for faster scraping
    GITHUB_TOKEN_1=ghp_secondaryTokenHere...
    GITHUB_TOKEN_2=ghp_tertiaryTokenHere...
    ```

4.  **Configure Target Repository:**
    Open `src/config.py` and set the `OWNER` and `REPO` variables to the repository you want to scrape (e.g., `stripe` and `stripe-python`).

## Usage

### 1. Run the Scraper
This fetches raw data from GitHub and saves it to the `data/` folder.
```bash
python -m src.scraper