import pandas as pd
import argparse
import os
import glob
from src import config

def load_latest_data():
    """Finds the most recent _FINAL.csv from the scraper."""
    search_path = os.path.join(config.OUTPUT_DIR, "*_FINAL.csv")
    files = glob.glob(search_path)
    if not files:
        raise FileNotFoundError(f"No '_FINAL.csv' files found in {config.OUTPUT_DIR}. Run scraper first.")
    latest_file = max(files, key=os.path.getctime)
    print(f"Loading data from: {latest_file}")
    return pd.read_csv(latest_file)

def prepare_dataframe(raw_df):
    """Common setup: datetime conversion and bot filtering."""
    df = raw_df.copy()
    df['created_at'] = pd.to_datetime(df['created_at'], utc=True)

    # bot filter
    bot_pat = '|'.join(config.FILTER_BOT_KEYWORDS)
    df = df[~df['author_username'].str.contains(bot_pat, case=False, na=False)]
    
    return df

def export_clarityloop_files(df, raw_df, prefix=""):
    """
    Shared function to generate the 5 CSVs.
    Args:
        df: The filtered DataFrame to export.
        raw_df: The original full DataFrame (needed for mapping parent URLs).
        prefix: Optional prefix for filenames (e.g., 'ltc_').
    """
    if df.empty:
        print(f"Dataset empty. No files generated for prefix '{prefix}'.")
        return

    print(f"\nGenerating CSVs with prefix '{prefix}'...")
    
    # ensure email column exists
    if 'author_email_fake' not in df.columns:
        df['author_email_fake'] = df['author_username'] + '@' + config.TARGET_EMAIL_DOMAIN

    # 1. workspaces
    ws = df[['workspace_name', 'workspace_title']].drop_duplicates().copy()
    ws['owner_email'] = f"owner@{config.TARGET_EMAIL_DOMAIN}"
    ws = ws.rename(columns={'workspace_title': 'title'})
    ws.to_csv(os.path.join(config.OUTPUT_DIR, f'{prefix}workspaces.csv'), index=False)

    # 2. users
    us = df[['author_full_name', 'author_email_fake']].drop_duplicates('author_email_fake').copy()
    us = us.rename(columns={'author_full_name': 'name', 'author_email_fake': 'email'})
    us['gender'] = 'UNKNOWN'
    us['ethnicity'] = 'UNKNOWN'
    us.to_csv(os.path.join(config.OUTPUT_DIR, f'{prefix}users.csv'), index=False)

    # 3. members
    mem = df[['workspace_name', 'author_email_fake']].drop_duplicates().copy()
    mem = mem.rename(columns={'author_email_fake': 'user_email'})
    mem['role'] = 'MEMBER'
    mem['title'] = 'Contributor'
    mem['manager_email'] = f"manager@{config.TARGET_EMAIL_DOMAIN}"
    mem.to_csv(os.path.join(config.OUTPUT_DIR, f'{prefix}workspace_members.csv'), index=False)

    # 4. contexts (cases/PRs)
    ctx = df[df['parent_id'].isna()].copy()
    ctx = ctx.rename(columns={
        'author_email_fake': 'author_email', 'url': 'link', 'text_content': 'body',
        'author_username': 'user', 'collaborators_fake': 'collaborators'
    })
    ctx['description'] = ctx['body'].str.slice(0, 200) + '...'
    for col in ['author', 'content', 'key', 'reporter']: ctx[col] = None
    
    ctx_cols = ['workspace_name', 'author_email', 'link', 'context_type', 'title', 'created_at',
                'user', 'description', 'body', 'author', 'content', 'key', 'reporter', 'collaborators']
    ctx[ctx_cols].to_csv(os.path.join(config.OUTPUT_DIR, f'{prefix}contexts.csv'), index=False)

    # 5. comments
    com = df[df['parent_id'].notna()].copy()
    # create mapping from RAW dataframe to ensure we find parents even if parent was filtered out
    parent_urls = raw_df.drop_duplicates('record_id').set_index('record_id')['url']
    
    com['context_link'] = com['parent_id'].map(parent_urls)
    com = com.rename(columns={
        'author_email_fake': 'comment_author_email',
        'text_content': 'comment_content',
        'url': 'comment_link'
    })
    com[['context_link', 'comment_author_email', 'comment_content', 'comment_link']].to_csv(os.path.join(config.OUTPUT_DIR, f'{prefix}context_comments.csv'), index=False)

    print(f"-> {prefix}workspaces.csv: {len(ws)}")
    print(f"-> {prefix}users.csv: {len(us)}")
    print(f"-> {prefix}workspace_members.csv: {len(mem)}")
    print(f"-> {prefix}contexts.csv: {len(ctx)}")
    print(f"-> {prefix}context_comments.csv: {len(com)}")

def print_stats(final_df):
    """Prints richness analysis stats."""
    print("\n" + "="*50)
    print("       DATASET RICHNESS ANALYSIS")
    print("="*50)

    if final_df.empty: return

    # top users
    print("\n--- Top 20 Users by Contribution & Engagement ---")
    case_feedback = final_df[final_df['parent_id'].notna()].groupby('thread_id').size().reset_index(name='fb_count')
    cases = final_df[final_df['parent_id'].isna()].merge(case_feedback, on='thread_id', how='left').fillna(0)

    user_richness = cases.groupby(['author_username', 'author_email_fake']).agg(
        total_cases=('thread_id', 'count'),
        avg_feedback=('fb_count', 'mean'),
        total_feedback=('fb_count', 'sum')
    ).sort_values(by='total_cases', ascending=False)
    
    print(user_richness.head(20).to_string())

    # feedback only
    print("\n--- 'Feedback-Only' Members ---")
    all_authors = set(final_df['author_username'].unique())
    case_starters = set(final_df[final_df['parent_id'].isna()]['author_username'].unique())
    fb_only = list(all_authors - case_starters)
    
    print(f"Count: {len(fb_only)}")
    if fb_only: print(f"Sample: {', '.join(sorted(fb_only)[:20])}...")
    print("\n" + "="*50)


# Pipeline 1: standard filtering
def run_standard_pipeline(raw_df):
    print("\n--- Running STANDARD Pipeline ---")
    
    # 1. common prep (datetime & bots)
    df = prepare_dataframe(raw_df)

    # 2. time filter
    if config.FILTER_TIME_CUTOFF_MONTHS > 0:
        cutoff = pd.Timestamp.now(tz='UTC') - pd.DateOffset(months=config.FILTER_TIME_CUTOFF_MONTHS)
        df = df[df['created_at'] >= cutoff]

    # 3. quality filters
    comments = df[df['type'] == 'comment']
    thread_counts = comments.groupby('thread_id').size()
    valid_threads = thread_counts[thread_counts >= config.FILTER_MIN_COMMENTS_PER_CASE].index

    case_starters = df[df['type'].isin(['issue_body', 'pull_request_body'])]
    user_counts = case_starters.groupby('author_id').size()
    valid_users = user_counts[user_counts >= config.FILTER_MIN_CASES_PER_USER].index

    # 4. assemble
    df_valuable_threads = df[df['thread_id'].isin(valid_threads)]
    active_threads = case_starters[case_starters['author_id'].isin(valid_users)]['thread_id']
    df_active_users = df[df['thread_id'].isin(active_threads)]

    final_df = pd.concat([df_valuable_threads, df_active_users]).drop_duplicates(subset=['record_id']).sort_values(by=['thread_id', 'created_at'])

    export_clarityloop_files(final_df, raw_df, prefix="")
    print_stats(final_df)

# Pipeline 2: LTC filtering 
def run_ltc_pipeline(raw_df):
    print("\n--- Running LONG-TERM CONTRIBUTOR Pipeline ---")
    
    # 1. common prep (datetime & bots)
    df = prepare_dataframe(raw_df)

    # 2. identify consistent users
    latest_date = df['created_at'].max()
    cutoff_date = latest_date - pd.DateOffset(years=config.LTC_MIN_YEARS_ACTIVE)
    
    case_starters = df[df['type'].isin(['issue_body', 'pull_request_body'])]
    consistent_users = []

    print(f"Checking consistency for {config.LTC_MIN_YEARS_ACTIVE} years...")
    
    for username, group in case_starters.groupby('author_username'):
        is_consistent = True
        for i in range(config.LTC_MIN_YEARS_ACTIVE):
            w_end = latest_date - pd.DateOffset(years=i)
            w_start = latest_date - pd.DateOffset(years=i+1)
            
            has_case = not group[(group['created_at'] > w_start) & (group['created_at'] <= w_end)].empty
            if not has_case:
                is_consistent = False
                break
        if is_consistent:
            consistent_users.append(username)

    print(f"Found {len(consistent_users)} consistent users.")

    # 3. filter data
    ltc_df = df[df['author_username'].isin(consistent_users)].copy()
    ltc_df = ltc_df[ltc_df['created_at'] > cutoff_date]

    # 4. quality control
    comments = ltc_df[ltc_df['type'] == 'comment']
    valid_threads = comments.groupby('thread_id').size()
    valid_ids = valid_threads[valid_threads >= config.LTC_MIN_COMMENTS_QUALITY].index
    
    final_df = ltc_df[ltc_df['thread_id'].isin(valid_ids)].copy()

    export_clarityloop_files(final_df, raw_df, prefix="ltc_")
    print_stats(final_df)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process ClarityLoop Datasets")
    parser.add_argument('--mode', choices=['standard', 'ltc', 'all'], default='all', help="Which pipeline to run")
    args = parser.parse_args()

    raw_data = load_latest_data()

    if args.mode in ['standard', 'all']:
        run_standard_pipeline(raw_data)
    
    if args.mode in ['ltc', 'all']:
        run_ltc_pipeline(raw_data)