import pandas as pd
import random
import os
import glob
from faker import Faker
from src import config

fake = Faker()

def generate_human_name(row):
    """Generates realistic names based on username or gender."""
    username = str(row['email']).split('@')[0]
    current_name = str(row['name']) if pd.notna(row['name']) else ""
    gender = row.get('gender')

    # 1. use existing name if valid
    if (current_name and
        current_name.lower() != username.lower() and
        ' ' in current_name):
        return current_name

    # 2. derive from username
    clean_user = username.replace('-', ' ').replace('.', ' ').replace('_', ' ')
    if ' ' in clean_user and not any(char.isdigit() for char in clean_user):
        return clean_user.title()

    # 3. generate fake name
    if gender == 'MALE':
        return f"{fake.first_name_male()} {fake.last_name()}"
    elif gender == 'FEMALE':
        return f"{fake.first_name_female()} {fake.last_name()}"
    else:
        return f"{fake.first_name()} {fake.last_name()}"

def fix_collaborators(row):
    """Formats collaborator list into clean emails."""
    raw_collabs = str(row['collaborators'])
    if pd.isna(row['collaborators']) or raw_collabs.strip() == '':
        return ''

    try:
        domain = str(row['author_email']).split('@')[1]
    except IndexError:
        domain = "github.com"

    cleaned_emails = []
    for u in raw_collabs.split(','):
        u = u.strip()
        if not u: continue
        
        # filter bots
        if ('[bot]' in u.lower() or u.lower().endswith('-bot') or 
            u.lower().startswith('bot-') or u == 'github-actions'):
            continue

        cleaned_emails.append(f"{u}@{domain}")

    return ",".join(cleaned_emails)

def clean_dataset_group(prefix=""):
    """Applies cleaning logic to a specific set of CSVs (standard or ltc)."""
    print(f"\n--- Cleaning files with prefix '{prefix}' ---")
    
    # 1. fix contexts
    ctx_path = os.path.join(config.OUTPUT_DIR, f'{prefix}contexts.csv')
    if os.path.exists(ctx_path):
        df = pd.read_csv(ctx_path)
        df['description'] = df['title']
        df['collaborators'] = df.apply(fix_collaborators, axis=1)
        df.to_csv(ctx_path, index=False)
        print(f"-> {prefix}contexts.csv updated.")
    else:
        print(f"[!] {prefix}contexts.csv not found.")

    # 2. fix users
    user_path = os.path.join(config.OUTPUT_DIR, f'{prefix}users.csv')
    if os.path.exists(user_path):
        df = pd.read_csv(user_path)
        
        # set demographics
        genders = ['MALE', 'FEMALE']
        ethnicities = ['CAUCASIAN', 'ASIAN', 'HISPANIC', 'AFRICAN_AMERICAN']
        
        df['gender'] = df['gender'].apply(lambda x: random.choice(genders))
        df['ethnicity'] = df['ethnicity'].apply(lambda x: random.choice(ethnicities))
        
        # set names
        df['name'] = df.apply(generate_human_name, axis=1)
        
        df.to_csv(user_path, index=False)
        print(f"-> {prefix}users.csv updated.")
    else:
        print(f"[!] {prefix}users.csv not found.")

    # 3. fix members
    mem_path = os.path.join(config.OUTPUT_DIR, f'{prefix}workspace_members.csv')
    if os.path.exists(mem_path):
        df = pd.read_csv(mem_path)
        df['role'] = df['role'].fillna('MEMBER')
        df.to_csv(mem_path, index=False)
        print(f"-> {prefix}workspace_members.csv verified.")
    else:
        print(f"[!] {prefix}workspace_members.csv not found.")

def main():
    print("Starting post-processing...")
    
    # Check for standard files
    if os.path.exists(os.path.join(config.OUTPUT_DIR, 'users.csv')):
        clean_dataset_group(prefix="")
        
    # Check for LTC files
    if os.path.exists(os.path.join(config.OUTPUT_DIR, 'ltc_users.csv')):
        clean_dataset_group(prefix="ltc_")
        
    print("\nPost-processing complete.")

if __name__ == "__main__":
    main()