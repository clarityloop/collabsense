# Validation Exercise — CollabSense Engine Outputs

## Purpose

We need independent human validation that the ClarityLoop engine's outputs (sentiment scores, identified strengths, and growth opportunities) are reasonable and grounded in the source data. Two raters (Sachin and Nima) will each independently review the same samples, and we will calculate inter-rater agreement to report in the paper.

## What to do

There are **3 CSV files** to review. Open each in Excel/Google Sheets, read the source comment, then fill in the rating columns.

### 1. `validation_strengths.csv` — 30 samples

The engine identified these as "strengths" for a person. For each row:

- Read the **Source_Comment** (the actual GitHub comment the engine based its assessment on)
- Look at **Engine_Label** (what the engine called the strength, e.g. "Engages in thoughtful discussions")
- Fill in:
  - **RATER_NAME**: Your name
  - **Q1_Label_Appropriate**: Does this label accurately describe what the person is doing in the comment? (`Yes` / `Partial` / `No`)
  - **Q2_Comment_Justifies_Label**: Does the comment content actually support this being called a "strength"? (`Yes` / `Partial` / `No`)
  - **Q3_Sentiment_Reasonable**: The engine assigned a sentiment score — does it feel right for this comment? (`Too_High` / `About_Right` / `Too_Low`)
  - **Q4_Notes**: Optional — anything you want to flag

### 2. `validation_growth_opportunities.csv` — 20 samples

The engine identified these as "growth opportunities" (areas where a person could improve). For each row:

- Read the **Source_Comment**
- Look at **Engine_Label** (the growth area identified)
- Fill in:
  - **RATER_NAME**: Your name
  - **Q1_Actionable_Feedback**: Is this something the person could actually act on? (`Yes` / `Partial` / `No`)
  - **Q2_Genuine_Growth_Area**: Does this represent a real area for improvement (not just a normal interaction)? (`Yes` / `Partial` / `No`)
  - **Q3_Based_On_Evidence**: Is this growth opportunity actually supported by what's in the comment? (`Yes` / `Partial` / `No`)
  - **Q4_Notes**: Optional

### 3. `validation_sentiment.csv` — 30 samples

The engine scored each comment on a 0–10 sentiment scale. For each row:

- Read the **Source_Comment**
- Fill in:
  - **RATER_NAME**: Your name
  - **Q1_Your_Sentiment_Score**: Your own 0–10 score for this comment (0 = very negative, 5 = neutral, 10 = very positive)
  - **Q2_Engine_Score_Reasonable**: Is the engine's score reasonable? (`Yes` / `Close` / `No`)
  - **Q3_Notes**: Optional

## Important

- **Work independently** — don't discuss your ratings with the other rater until both are done
- **Put your name** in the RATER_NAME column of every row you rate
- Each rater should fill in their own copy of the files (e.g. save as `validation_strengths_sachin.csv` and `validation_strengths_nima.csv`)
- If a source comment is truncated, use the GitHub link to read the full comment
- There are no right or wrong answers — we want your honest assessment

## After completion

Return the completed CSVs. We will calculate:
- **Agreement rate** between the two raters (% of items rated the same)
- **Cohen's Kappa** for inter-rater reliability
- These metrics will be reported in the paper's methodology section

## Time estimate

~80 items total across 3 files. Should take roughly 45–60 minutes per rater.
