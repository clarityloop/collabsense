import pandas as pd
import argparse
import os
import glob
import networkx as nx
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
    """Common setup: datetime conversion."""
    df = raw_df.copy()
    df['created_at'] = pd.to_datetime(df['created_at'], utc=True)
    # Note: We do NOT filter bots here. They are needed for comment context.
    return df

def export_clarityloop_files(df, raw_df, prefix=""):
    """
    Shared function to generate the 5 CSVs.
    """
    if df.empty:
        print(f"Dataset empty. No files generated for prefix '{prefix}'.")
        return

    print(f"\nGenerating CSVs with prefix '{prefix}'...")
    
    # ensure email column exists
    if 'author_email_fake' not in df.columns:
        df['author_email_fake'] = df['author_username'] + '@' + config.TARGET_EMAIL_DOMAIN

    # --- BOT FILTERING (Selective) ---
    # create a human-only view for Users/Members/Contexts
    bot_pat = '|'.join(config.FILTER_BOT_KEYWORDS)
    is_bot = df['author_username'].str.contains(bot_pat, case=False, na=False)
    human_df = df[~is_bot]

    # 1. workspaces (humans only)
    ws = human_df[['workspace_name', 'workspace_title']].drop_duplicates().copy()
    ws['owner_email'] = f"owner@{config.TARGET_EMAIL_DOMAIN}"
    ws = ws.rename(columns={'workspace_title': 'title'})
    ws.to_csv(os.path.join(config.OUTPUT_DIR, f'{prefix}workspaces.csv'), index=False)

    # 2. users (humans only)
    us = human_df[['author_full_name', 'author_email_fake']].drop_duplicates('author_email_fake').copy()
    us = us.rename(columns={'author_full_name': 'name', 'author_email_fake': 'email'})
    us['gender'] = 'UNKNOWN'
    us['ethnicity'] = 'UNKNOWN'
    us.to_csv(os.path.join(config.OUTPUT_DIR, f'{prefix}users.csv'), index=False)

    # 3. members (humans only)
    mem = human_df[['workspace_name', 'author_email_fake']].drop_duplicates().copy()
    mem = mem.rename(columns={'author_email_fake': 'user_email'})
    mem['role'] = 'MEMBER'
    mem['title'] = 'Contributor'
    mem['manager_email'] = f"manager@{config.TARGET_EMAIL_DOMAIN}"
    mem.to_csv(os.path.join(config.OUTPUT_DIR, f'{prefix}workspace_members.csv'), index=False)

    # 4. contexts (cases started by humans only)
    ctx = human_df[human_df['parent_id'].isna()].copy()
    ctx = ctx.rename(columns={
        'author_email_fake': 'author_email', 'url': 'link', 'text_content': 'body',
        'author_username': 'user', 'collaborators_fake': 'collaborators'
    })
    ctx['description'] = ctx['body'].str.slice(0, 200) + '...'
    for col in ['author', 'content', 'key', 'reporter']: ctx[col] = None
    
    ctx_cols = ['workspace_name', 'author_email', 'link', 'context_type', 'title', 'created_at',
                'user', 'description', 'body', 'author', 'content', 'key', 'reporter', 'collaborators']
    ctx[ctx_cols].to_csv(os.path.join(config.OUTPUT_DIR, f'{prefix}contexts.csv'), index=False)

    # 5. comments (INCLUDE BOTS - use full 'df')
    com = df[df['parent_id'].notna()].copy()
    
    # mapping parent URLs
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

    # exclude bots for stats calculation to see real human engagement
    bot_pat = '|'.join(config.FILTER_BOT_KEYWORDS)
    human_df = final_df[~final_df['author_username'].str.contains(bot_pat, case=False, na=False)]

    # Top Users
    print("\n--- Top 20 Users by Contribution & Engagement ---")
    case_feedback = human_df[human_df['parent_id'].notna()].groupby('thread_id').size().reset_index(name='fb_count')
    cases = human_df[human_df['parent_id'].isna()].merge(case_feedback, on='thread_id', how='left').fillna(0)

    user_richness = cases.groupby(['author_username', 'author_email_fake']).agg(
        total_cases=('thread_id', 'count'),
        avg_feedback=('fb_count', 'mean'),
        total_feedback=('fb_count', 'sum')
    ).sort_values(by='total_cases', ascending=False)
    
    print(user_richness.head(20).to_string())
    print("\n" + "="*50)


# pipeline 1: standard filtering
def run_standard_pipeline(raw_df):
    print("\n--- Running STANDARD Pipeline ---")
    
    # 1. common prep
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

# pipeline 2: LTC filtering 
def run_ltc_pipeline(raw_df):
    print("\n--- Running LONG-TERM CONTRIBUTOR Pipeline ---")
    
    # 1. common prep
    df = prepare_dataframe(raw_df)

    # 2. identify consistent users
    latest_date = df['created_at'].max()
    cutoff_date = latest_date - pd.DateOffset(years=config.LTC_MIN_YEARS_ACTIVE)
    
    case_starters = df[df['type'].isin(['issue_body', 'pull_request_body'])]
    consistent_users = []

    print(f"Checking consistency for {config.LTC_MIN_YEARS_ACTIVE} years...")
    
    # We only check consistency for HUMANS, so filter bots temporarily for this check
    bot_pat = '|'.join(config.FILTER_BOT_KEYWORDS)
    human_case_starters = case_starters[~case_starters['author_username'].str.contains(bot_pat, case=False, na=False)]

    for username, group in human_case_starters.groupby('author_username'):
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
    # Keep records where author is consistent user OR is a comment on their thread
    ltc_df = df[df['author_username'].isin(consistent_users)].copy()
    ltc_df = ltc_df[ltc_df['created_at'] > cutoff_date]

    # 4. quality control
    comments = ltc_df[ltc_df['type'] == 'comment']
    valid_threads = comments.groupby('thread_id').size()
    valid_ids = valid_threads[valid_threads >= config.LTC_MIN_COMMENTS_QUALITY].index
    
    final_df = ltc_df[ltc_df['thread_id'].isin(valid_ids)].copy()

    export_clarityloop_files(final_df, raw_df, prefix="ltc_")
    print_stats(final_df)

# pipeline 3: interaction density filtering
def run_dense_pipeline(raw_df):
    print("\n--- Running DENSITY CLIQUE Pipeline ---")
    
    # 1. Prep
    df = prepare_dataframe(raw_df)
    
    # 2. Build the Interaction Graph
    print("Building interaction graph...")
    
    # We only care about comments to build the graph (who talks to whom)
    comments = df[df['type'] == 'comment'].copy()
    
    # Map comments back to thread author
    case_starters = df[df['type'].isin(['issue_body', 'pull_request_body'])]
    thread_author_map = case_starters.set_index('thread_id')['author_username'].to_dict()
    
    comments['recipient'] = comments['thread_id'].map(thread_author_map)
    comments = comments.dropna(subset=['recipient'])
    
    # Filter out self-talk
    comments = comments[comments['author_username'] != comments['recipient']]
    
    # Filter out BOT interactions for the graph (we want human density)
    bot_pat = '|'.join(config.FILTER_BOT_KEYWORDS)
    comments = comments[~comments['author_username'].str.contains(bot_pat, case=False, na=False)]
    comments = comments[~comments['recipient'].str.contains(bot_pat, case=False, na=False)]

    # Create Graph
    G = nx.Graph()
    for _, row in comments.iterrows():
        sender = row['author_username']
        recipient = row['recipient']
        if G.has_edge(sender, recipient):
            G[sender][recipient]['weight'] += 1
        else:
            G.add_edge(sender, recipient, weight=1)
            
    print(f"Human Graph: {G.number_of_nodes()} users, {G.number_of_edges()} connections.")
    
    # 3. Apply K-Core Decomposition
    TARGET_TEAM_SIZE = 50
    best_k = 0
    core_nodes = []
    
    for k in range(1, 100):
        try:
            core = nx.k_core(G, k=k)
            num_nodes = core.number_of_nodes()
            if num_nodes == 0: break
            
            best_k = k
            core_nodes = list(core.nodes())
            
            if num_nodes <= TARGET_TEAM_SIZE: break
        except: break
            
    print(f"Selected k-core: k={best_k} (Each user interacts with at least {best_k} others)")
    print(f"Artificial Team Size: {len(core_nodes)} users")
    
    if len(core_nodes) > 0:
        subgraph = G.subgraph(core_nodes)
        density = nx.density(subgraph)
        print(f"Artificial Team Density: {density:.2%} (Target: >30%)")

    # 4. Filter the Dataset
    # Keep records where the AUTHOR is in the core_nodes list
    # AND include bot comments if they belong to these threads
    
    # First, find the threads started by the core team
    core_threads_df = case_starters[case_starters['author_username'].isin(core_nodes)].copy()

    # apply hard cap if set
    if config.DENSITY_MAX_CONTEXTS_PER_USER > 0:
        print(f"Applying cap: Max {config.DENSITY_MAX_CONTEXTS_PER_USER} contexts per user...")
        # Sort by date descending (newest first)
        core_threads_df = core_threads_df.sort_values('created_at', ascending=False)
        # Take top N per author
        core_threads_df = core_threads_df.groupby('author_username').head(config.DENSITY_MAX_CONTEXTS_PER_USER)
    
    core_thread_ids = core_threads_df['thread_id']

    # Select all records belonging to these threads
    dense_df = df[df['thread_id'].isin(core_thread_ids)].copy()
    
    # 5. Quality Control
    comments = dense_df[dense_df['type'] == 'comment']
    valid_threads = comments.groupby('thread_id').size()
    valid_ids = valid_threads[valid_threads >= config.FILTER_MIN_COMMENTS_PER_CASE].index
    
    final_df = dense_df[dense_df['thread_id'].isin(valid_ids)].copy()

    export_clarityloop_files(final_df, raw_df, prefix="dense_")
    print_stats(final_df)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process ClarityLoop Datasets")
    parser.add_argument('--mode', choices=['standard', 'ltc', 'dense', 'all'], default='all', help="Which pipeline to run")
    args = parser.parse_args()

    raw_data = load_latest_data()

    if args.mode in ['standard', 'all']:
        run_standard_pipeline(raw_data)
    
    if args.mode in ['ltc', 'all']:
        run_ltc_pipeline(raw_data)
        
    if args.mode in ['dense', 'all']:
        run_dense_pipeline(raw_data)