import asyncio
import aiohttp
import pandas as pd
import os
import time
import datetime
import traceback
from tqdm.asyncio import tqdm
from dotenv import load_dotenv
from src import config

# environment variables from .env file
load_dotenv()

# global buffer
GLOBAL_RESULTS_BUFFER = []
user_profile_cache = {}
repo_details_cache = {}

# ensure output directory exists
os.makedirs(config.OUTPUT_DIR, exist_ok=True)

class SmartTokenManager:
    """Manages multiple GitHub tokens to handle rate limits automatically."""
    def __init__(self):
        self.token_data = []
        self.current_index = 0
        self.lock = asyncio.Lock()

        # load token(s)
        i = 1
        while True:
            t = os.getenv(f'GITHUB_TOKEN_{i}')
            if t:
                self.token_data.append({'token': t, 'reset_at': 0})
            else:
                break
            i += 1

        if not self.token_data:
            t = os.getenv('GITHUB_TOKEN')
            if t:
                self.token_data.append({'token': t, 'reset_at': 0})

        if not self.token_data:
            raise ValueError("No GitHub tokens found in .env file!")

        print(f"Token Manager: Loaded {len(self.token_data)} tokens.")

    def get_current_headers(self):
        t_data = self.token_data[self.current_index]
        return {
            'Authorization': f'token {t_data["token"]}',
            'Accept': 'application/vnd.github.v3+json'
        }

    async def report_403_and_rotate(self, failed_index, github_reset_header):
        async with self.lock:
            current_time = time.time()
            # update reset time for failed token
            self.token_data[failed_index]['reset_at'] = int(github_reset_header) + 10

            # if index changed while waiting, check if new token is valid
            if self.current_index != failed_index:
                if self.token_data[self.current_index]['reset_at'] <= current_time:
                    return True, 0

            # find next available token
            for i, t_data in enumerate(self.token_data):
                if t_data['reset_at'] <= current_time:
                    self.current_index = i
                    print(f"\n[!] Switching to valid Token #{i + 1}...")
                    return True, 0

            # all tokens exhausted
            earliest_reset = min(t['reset_at'] for t in self.token_data)
            wait_time = max(earliest_reset - current_time, 0)
            print(f"\n[!] All tokens exhausted. Sleeping {wait_time/60:.1f} mins.")
            return False, wait_time

token_manager = SmartTokenManager()

def save_checkpoint(reason="CHECKPOINT"):
    if not GLOBAL_RESULTS_BUFFER: return

    print(f"\n[SAVE] Saving {reason}...")
    try:
        df = pd.DataFrame(GLOBAL_RESULTS_BUFFER)
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'])
            df.sort_values(by=['created_at', 'record_id'], ascending=[False, True], inplace=True)

        filename = os.path.join(config.OUTPUT_DIR, f'github_{config.OWNER}_{config.REPO}_{reason}.csv')
        df.to_csv(filename, index=False)
    except Exception as e:
        print(f"[ERROR] Save failed: {e}")

async def fetch_json(session, url, retries=3):
    if not url: return None, None

    for attempt in range(retries):
        try:
            attempt_index = token_manager.current_index
            headers = token_manager.get_current_headers()

            async with session.get(url, headers=headers) as response:
                if response.status == 403:
                    limit_remaining = response.headers.get('X-RateLimit-Remaining')
                    if limit_remaining == '0':
                        reset_header = response.headers.get('X-RateLimit-Reset', 0)
                        should_retry, wait_time = await token_manager.report_403_and_rotate(attempt_index, reset_header)
                        
                        if should_retry: continue
                        
                        async with token_manager.lock:
                            current_time = time.time()
                            earliest = min(t['reset_at'] for t in token_manager.token_data)
                            real_wait = max(earliest - current_time, 0)
                            if real_wait > 0:
                                save_checkpoint("RATE_LIMIT_PAUSE")
                                await asyncio.sleep(real_wait)
                            continue

                if response.status == 200:
                    return await response.json(), response.links
                if response.status == 404:
                    return None, None
                
                response.raise_for_status()

        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt == retries - 1: return None, None
            await asyncio.sleep(2)
        except Exception as e:
            print(f"Error in fetch_json: {e}")
            return None, None
    return None, None

async def fetch_paginated_async(session, start_url, max_pages=0, desc="Fetching", use_progress=False):
    all_items = []
    url = start_url
    page_count = 0
    pbar = tqdm(desc=f"{desc} (Pages)", unit="page", leave=False) if use_progress else None

    try:
        while url:
            if max_pages != 0 and page_count >= max_pages: break
            data, links = await fetch_json(session, url)
            if not data: break
            
            all_items.extend(data)
            
            if 'next' in links:
                url = links['next']['url']
                page_count += 1
                if pbar: pbar.update(1)
            else:
                break
    finally:
        if pbar: pbar.close()
    return all_items

async def get_user_full_name_async(session, username):
    if not username: return None
    if username in user_profile_cache: return user_profile_cache[username]
    data, _ = await fetch_json(session, f"https://api.github.com/users/{username}")
    full_name = data.get('name') if data else None
    user_profile_cache[username] = full_name
    return full_name

async def process_thread(session, issue, semaphore):
    try:
        async with semaphore:
            interactions = []
            is_pr = 'pull_request' in issue

            # prepare async tasks
            tasks = [get_user_full_name_async(session, issue['user']['login'])]
            
            if is_pr:
                tasks.append(fetch_json(session, issue['pull_request']['url']))
                tasks.append(fetch_paginated_async(session, f"{issue['pull_request']['url']}/reviews", use_progress=False))
            else:
                tasks.extend([asyncio.sleep(0), asyncio.sleep(0)])

            if issue['comments'] > 0:
                tasks.append(fetch_paginated_async(session, issue['comments_url'], use_progress=False))
            else:
                tasks.append(asyncio.sleep(0))

            results = await asyncio.gather(*tasks)
            author_full_name = results[0]
            
            # PR stats
            pr_stats = {'commits': None, 'changed_files': None, 'additions': None, 'deletions': None}
            collaborators_set = {issue['user']['login']}

            if is_pr and results[1] and results[1][0]:
                data = results[1][0]
                for k in pr_stats.keys(): pr_stats[k] = data.get(k)

            if is_pr and isinstance(results[2], list):
                for review in results[2]:
                    if review.get('user'): collaborators_set.add(review['user']['login'])

            comments_data = results[3] if isinstance(results[3], list) else []

            # create main record
            interactions.append({
                'record_id': issue['id'], 'thread_id': issue['number'], 'parent_id': None,
                'repo': f"{config.OWNER}/{config.REPO}",
                'type': 'pull_request_body' if is_pr else 'issue_body',
                'author_id': issue['user']['id'], 'author_username': issue['user']['login'],
                'title': issue.get('title'), 'text_content': issue.get('body'),
                'created_at': issue['created_at'], 'url': issue['html_url'],
                **pr_stats,
                'workspace_name': config.OWNER,
                'workspace_title': repo_details_cache.get('desc'),
                'context_type': 'GITHUB_PR' if is_pr else 'GITHUB_ISSUE',
                'author_full_name': author_full_name,
                'author_email_fake': f"{issue['user']['login']}@{config.REPO}.com",
                'collaborators_fake': ""
            })

            # fetch comment author names in parallel
            comment_name_tasks = [get_user_full_name_async(session, c['user']['login']) if c.get('user') else asyncio.sleep(0) for c in comments_data]
            comment_names = await asyncio.gather(*comment_name_tasks)

            # create comment records
            for i, comment in enumerate(comments_data):
                if not comment.get('user'): continue
                username = comment['user']['login']
                if is_pr: collaborators_set.add(username)

                interactions.append({
                    'record_id': comment['id'], 'thread_id': issue['number'], 'parent_id': issue['id'],
                    'repo': f"{config.OWNER}/{config.REPO}", 'type': 'comment',
                    'author_id': comment['user']['id'], 'author_username': username,
                    'title': None, 'text_content': comment.get('body'), 'created_at': comment['created_at'],
                    'url': comment['html_url'],
                    'workspace_name': config.OWNER, 'workspace_title': repo_details_cache.get('desc'),
                    'context_type': None,
                    'author_full_name': comment_names[i],
                    'author_email_fake': f"{username}@{config.REPO}.com",
                    'collaborators_fake': ""
                })

            interactions[0]['collaborators_fake'] = ",".join(sorted(list(collaborators_set)))
            return interactions
    except Exception as e:
        print(f"Error thread {issue['number']}: {e}")
        return []

async def main():
    print(f"Targeting: {config.OWNER}/{config.REPO}")
    print(f"Saving to: {config.OUTPUT_DIR}")

    timeout = aiohttp.ClientTimeout(total=None)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # get repo details
            repo_data, _ = await fetch_json(session, f"https://api.github.com/repos/{config.OWNER}/{config.REPO}")
            repo_details_cache['desc'] = repo_data.get('description') if repo_data else None

            # get issue list
            issues_url = f'https://api.github.com/repos/{config.OWNER}/{config.REPO}/issues?state=all&per_page=100'
            issues_list = await fetch_paginated_async(session, issues_url, config.MAX_ISSUE_PAGES, desc="Fetching List", use_progress=True)

            print(f"\nProcessing {len(issues_list)} threads...")
            semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_REQUESTS)
            tasks = [process_thread(session, issue, semaphore) for issue in issues_list]

            completed = 0
            last_print = time.time()

            for f in asyncio.as_completed(tasks):
                try:
                    result = await f
                    GLOBAL_RESULTS_BUFFER.extend(result)
                    completed += 1
                    
                    if time.time() - last_print > 10:
                        print(f"Progress: {completed}/{len(issues_list)} ({completed/len(issues_list)*100:.1f}%)")
                        last_print = time.time()
                except Exception as e:
                    print(f"Task failed: {e}")

            if GLOBAL_RESULTS_BUFFER: save_checkpoint("FINAL")
            else: print("No data processed.")

    except Exception as e:
        print(f"CRITICAL: {e}")
        traceback.print_exc()
        save_checkpoint("CRASH_DUMP")

if __name__ == "__main__":
    asyncio.run(main())