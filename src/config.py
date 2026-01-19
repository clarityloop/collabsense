import os

OUTPUT_DIR = './data'

# Configuration
OWNER = 'pandas-dev'
REPO = 'pandas'

# Scraping Settings
MAX_ISSUE_PAGES = 0  # 0 for all
MAX_CONCURRENT_REQUESTS = 10

# Filter Settings (processor.py)
FILTER_TIME_CUTOFF_MONTHS = 24
FILTER_MIN_COMMENTS_PER_CASE = 2
LTC_MIN_YEARS_ACTIVE = 5