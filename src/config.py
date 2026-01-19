import os

BASE_DATA_DIR = './data'
OUTPUT_DIR = BASE_DATA_DIR

# Configuration
OWNER = 'pandas-dev'
REPO = 'pandas'

# Scraping Settings
MAX_ISSUE_PAGES = 0  # 0 for all
MAX_CONCURRENT_REQUESTS = 10

# Filter Settings (processor.py)
TARGET_EMAIL_DOMAIN = "example.com"
FILTER_TIME_CUTOFF_MONTHS = 24
FILTER_MIN_COMMENTS_PER_CASE = 2
FILTER_MIN_CASES_PER_USER = 4
FILTER_BOT_KEYWORDS = ['[bot]', '-bot', 'bot-']

# Long-Term Contributor Settings
LTC_MIN_YEARS_ACTIVE = 3
LTC_MIN_COMMENTS_QUALITY = 2