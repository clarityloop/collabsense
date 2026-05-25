# CollabSense

An empirical evaluation of the [ClarityLoop](https://clarityloop.com) AI engine on open source software collaboration data. This research investigates whether an enterprise-oriented behavioural coaching system can extract stable sentiment scores, individual strengths, and actionable growth opportunities from public GitHub interactions.

## Overview

CollabSense uses a custom data pipeline to scrape, filter, and anonymise public GitHub pull request and issue data, then processes it through the ClarityLoop engine across multiple experimental configurations varying prompt sensitivity and interaction graph density. The study evaluates the engine against data from **pandas-dev/pandas** and **kubernetes/kubernetes**.

Key findings include:
- Stable sentiment scoring across all configurations (mean 6.63/10)
- Growth opportunities emerge when interaction density is engineered via K-Core decomposition
- Manual validation by two reviewers found 85% of sampled growth opportunities at least partially actionable (Cohen's κ = 0.76)

## Repository Structure

```text
collabsense/
├── dataset/           # Compiled results (feedback scores, strengths, growth opportunities)
├── reports/           # Paper LaTeX source and analysis outputs
├── src/               # Data pipeline (scraper, processor, anonymiser)
└── requirements.txt   # Python dependencies
```

## Paper

The full paper is available at `reports/collabsense_paper_latex.tex`, targeting ESEM SEIP 2026 (Munich, October 4–9).

## Data Pipeline

To reproduce the data collection, install dependencies (`pip install -r requirements.txt`), configure GitHub tokens in a `.env` file, and run `python -m src.pipeline`. See `src/config.py` for target repository and filtering thresholds.
