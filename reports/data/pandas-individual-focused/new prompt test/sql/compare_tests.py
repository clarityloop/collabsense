import pandas as pd
import csv
import re
import os
import argparse
import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud

# ==============================================================================
# CONFIGURATION
# ==============================================================================
USER_FILE = 'user_backup.sql'
FEEDBACK_FILE = 'feedback_backup.sql'
# Change this if your table is named differently
GO_TABLE_NAME = 'growth_opportunity' 

# ==============================================================================
# PARSING LOGIC (Same as before)
# ==============================================================================
def parse_sql_file(filename, table_name, columns):
    if not os.path.exists(filename): return pd.DataFrame(columns=columns)
    data = []
    try:
        with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        pattern = f"INSERT INTO `{table_name}` VALUES (.*?);"
        matches = re.findall(pattern, content, re.DOTALL)
        for match in matches:
            rows = re.split(r"\),\s*\(", match)
            for row in rows:
                row_clean = row.strip('() \n\r')
                try:
                    reader = csv.reader([row_clean], quotechar="'", skipinitialspace=True)
                    parsed_row = next(reader)
                    if len(parsed_row) >= len(columns):
                        data.append(parsed_row[:len(columns)])
                except: continue
        return pd.DataFrame(data, columns=columns)
    except: return pd.DataFrame(columns=columns)

def extract_seed(email):
    if not email or '_' not in email: return '0'
    parts = email.split('_')
    return parts[0] if parts[0].isdigit() else '0'

# ==============================================================================
# VISUALIZATION LOGIC
# ==============================================================================
def generate_visuals(seed_a, seed_b):
    print("Loading Data...")
    
    # 1. Load Users (To map IDs to Seeds)
    user_cols = ['id', 'email', 'name', 'role', 'status', 'active', 'avatar', 'provider', 'pid', 'unlinked', 'created_at']
    df_users = parse_sql_file(USER_FILE, 'user_info', user_cols)
    df_users['seed'] = df_users['email'].apply(extract_seed)
    user_seed_map = dict(zip(df_users['id'], df_users['seed']))

    # 2. Load Growth Opportunities
    go_cols = ['id', 'workspace_id', 'user_id', 'title', 'active', 'created_at', 'cb', 'ub', 'ua']
    df_go = parse_sql_file(FEEDBACK_FILE, GO_TABLE_NAME, go_cols)
    
    # 3. Load Comments (For Sentiment)
    comment_cols = ['id', 'workspace_id', 'sender_user_id', 'sentiment_score', 'created_at', 'updated_at', 'recipient_user_id']
    df_comments = parse_sql_file(FEEDBACK_FILE, 'comment', comment_cols)

    # --- FILTERING ---
    df_go['seed'] = df_go['user_id'].map(user_seed_map)
    df_comments['seed'] = df_comments['sender_user_id'].map(user_seed_map)

    # Keep only target seeds
    df_go = df_go[df_go['seed'].isin([seed_a, seed_b])]
    df_comments = df_comments[df_comments['seed'].isin([seed_a, seed_b])]

    print(f"GO Count: Seed {seed_a}={len(df_go[df_go['seed']==seed_a])}, Seed {seed_b}={len(df_go[df_go['seed']==seed_b])}")

    # --- CHART 1: SENTIMENT STABILITY (KDE Plot) ---
    print("Generating Sentiment Chart...")
    plt.figure(figsize=(10, 6))
    df_comments['sentiment_score'] = pd.to_numeric(df_comments['sentiment_score'], errors='coerce')
    
    sns.kdeplot(data=df_comments[df_comments['seed']==seed_a], x='sentiment_score', label=f'Test A (Seed {seed_a})', fill=True, alpha=0.3)
    sns.kdeplot(data=df_comments[df_comments['seed']==seed_b], x='sentiment_score', label=f'Test B (Seed {seed_b})', fill=True, alpha=0.3)
    
    plt.title('Model Stability: Sentiment Distribution Comparison')
    plt.xlabel('Sentiment Score')
    plt.legend()
    plt.savefig('sentiment_stability_check.png')
    plt.close()

    # --- CHART 2: SPAMINESS (Histogram of GOs per User) ---
    print("Generating Spaminess Chart...")
    go_counts = df_go.groupby(['seed', 'user_id']).size().reset_index(name='count')
    
    plt.figure(figsize=(10, 6))
    sns.histplot(data=go_counts, x='count', hue='seed', multiple='dodge', shrink=.8, bins=range(1, 15))
    plt.title('Distribution of Growth Opportunities per User')
    plt.xlabel('Number of GOs Received')
    plt.ylabel('Count of Users')
    plt.xticks(range(1, 15))
    plt.savefig('go_distribution_per_user.png')
    plt.close()

    # --- CHART 3: WORD CLOUDS ---
    print("Generating Word Clouds...")
    fig, axes = plt.subplots(1, 2, figsize=(16, 8))
    
    for i, seed in enumerate([seed_a, seed_b]):
        text = " ".join(df_go[df_go['seed'] == seed]['title'].astype(str))
        if len(text) > 0:
            wc = WordCloud(width=800, height=400, background_color='white').generate(text)
            axes[i].imshow(wc, interpolation='bilinear')
            axes[i].set_title(f"Seed {seed} Topics")
            axes[i].axis('off')
        else:
            axes[i].text(0.5, 0.5, "No Data", ha='center')
    
    plt.savefig('go_wordcloud_comparison.png')
    plt.close()
    
    print("Done! Charts saved.")

if __name__ == "__main__":
    generate_visuals('4', '5')