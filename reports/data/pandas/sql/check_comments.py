import csv
import json
import pandas as pd

# 1. Load Scores & Sender IDs
comment_data = {}
print("Loading Scores...")
with open('feedback_backup.sql', 'r', encoding='utf-8', errors='ignore') as f:
    for line in f:
        if "INSERT INTO `comment`" in line:
            clean_line = line[line.find("(")+1 : line.rfind(")")]
            rows = clean_line.split("),(")
            for r in rows:
                try:
                    # ID=0, SenderID=2, Score=3
                    parts = r.split(",")
                    cid = parts[0]
                    sender_id = parts[2]
                    score = parts[3]
                    comment_data[cid] = {'score': int(score), 'sender_id': sender_id}
                except: continue

# 2. Load User Names (ID -> Name)
user_map = {}
print("Loading Users...")
with open('user_backup.sql', 'r', encoding='utf-8', errors='ignore') as f:
    for line in f:
        if "INSERT INTO `user_info`" in line:
            clean_line = line[line.find("(")+1 : line.rfind(")")]
            rows = clean_line.split("),(")
            for r in rows:
                try:
                    # Use CSV reader for names with quotes
                    reader = csv.reader([r], quotechar="'")
                    cols = next(reader)
                    # ID=0, Name=2
                    user_map[cols[0]] = cols[2]
                except: continue

# 3. Load Text & Links
content_map = {}
print("Loading Text & Links...")
with open('workspace_backup.sql', 'r', encoding='utf-8', errors='ignore') as f:
    for line in f:
        if "`context_comment`" in line and "INSERT" in line:
            start = line.find("VALUES (") + 8
            end = line.rfind(");")
            records = line[start:end].split("),(")
            for r in records:
                try:
                    reader = csv.reader([r], quotechar="'")
                    cols = next(reader)
                    # ID=0, Link=2, Content=4
                    content_map[cols[0]] = {'link': cols[2], 'text': cols[4]}
                except: continue
        
        elif "`context`" in line and "INSERT" in line:
             # Similar logic for main PR bodies if you need them
             # (Skipping for brevity, usually comments are better examples)
             pass

# 4. Generate Markdown Table Rows
print("\n" + "="*50)
print("| Score | User | Comment Excerpt | Link |")
print("| :---: | :--- | :--- | :--- |")

for score in [4, 5, 6, 7, 8, 9]:
    # Find IDs
    ids = [k for k, v in comment_data.items() if v['score'] == score]
    
    # Filter for valid content
    valid_ids = [i for i in ids if i in content_map and len(content_map[i]['text']) > 50]
    
    # Take 2 examples
    for i in valid_ids[:3]:
        data = comment_data[i]
        content = content_map[i]
        
        user_name = user_map.get(data['sender_id'], "Unknown")
        text_preview = content['text'].replace("\n", " ")[:5000] + "..."
        link = f"[View]({content['link']})"
        
        print(f"| **{score}** | {user_name} | *\"{text_preview}\"* | {link} |")