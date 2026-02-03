import os

BASE_DATA_DIR = './data'
OUTPUT_DIR = BASE_DATA_DIR

# Configuration
OWNER = 'kubernetes'
REPO = 'kubernetes'

#pandas-dev/pandas, kubernetes/kubernetes, tensorflow/tensorflow, flutter/flutter

# Scraping Settings
MAX_ISSUE_PAGES = 0  # 0 for all
MAX_CONCURRENT_REQUESTS = 10

# Filter Settings (processor.py)
TARGET_EMAIL_DOMAIN = "example.com"
FILTER_TIME_CUTOFF_MONTHS = 24
FILTER_MIN_COMMENTS_PER_CASE = 2
FILTER_MIN_CASES_PER_USER = 4
FILTER_BOT_KEYWORDS = ['[bot]', '-bot', 'bot-']

# Interaction Density Settings
DENSITY_TARGET_TEAM_SIZE = 50
DENSITY_MAX_CONTEXTS_PER_USER = 50  # Set to 0 for unlimited

# Long-Term Contributor Settings
LTC_MIN_YEARS_ACTIVE = 3
LTC_MIN_COMMENTS_QUALITY = 2