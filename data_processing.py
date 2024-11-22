import pandas as pd
import os, json, gzip, aiohttp, asyncio
from datetime import datetime
import re, os, sys, time, psycopg2
from typing import Set, Optional
from urllib.parse import urlsplit
from aiofiles import open as aio_open

shorturl_services: Set[str] = set()
domain_scores = {}
db_config = {
    'host': os.environ['DB_HOST'],
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWORD'],
    'database': os.environ['DB_NAME'],
    'port': 5432,
}

try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
except Exception as e:
    print(f"Database connection error: {e}")
    exit(1)

async def load_domain_scores():
    try:
        metadata = pd.read_csv('./NewsGuard/metadata.csv', on_bad_lines='skip')
        all_sources = pd.read_csv('./NewsGuard/all-sources-metadata.csv', on_bad_lines='skip')
        
        domain_scores.update({
            row['Domain']: row['Score'] 
            for df in [all_sources, metadata] 
            for row in df.to_dict(orient='records')
        })
    except Exception as e:
        print(f"Error loading domains: {e}")

def load_short_url_domains(filepath: str) -> Set[str]:
    try:
        services = set(pd.read_csv(filepath).iloc[:, 0])
        return services
    except Exception as e:
        print(f"Error loading short URL domains: {e}")
        return set()

def is_short_url(url: str, short_domains: Set[str]) -> bool:
    return urlsplit(url).netloc in short_domains

def extract_domain(url: str) -> Optional[str]:
    if not url:
        return None
    match = re.match(r"(?:https?://)?(?:www\d?\.)?([^/]+)", url)
    return match.group(1).lower() if match else None

async def unshorten_url(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    try:
        async with session.get(url, allow_redirects=False) as response:
            if 300 <= response.status < 400 and "Location" in response.headers:
                resolved_url = response.headers["Location"]
                return resolved_url
            return str(response.real_url)
    except Exception as e:
        print(f"Error unshortening URL {url}: {e}")
        return None

def wait_for_file(filepath: str, max_wait_time: int = 60):
    start_time = time.time()
    attempt = 0
    while True:
        attempt += 1
        try:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if not os.path.exists(filepath):
                print(f"[{current_time}] File {filepath} does not exist. Retrying...")
                time.sleep(0.5)
                continue
            if filepath.endswith(".gz"):
                with gzip.open(filepath, 'rt', encoding='utf-8') as f:
                    json.load(f)
            else:
                with open(filepath, 'r', encoding='utf-8') as f:
                    json.load(f)
            break

        except (OSError, json.JSONDecodeError) as e:
            elapsed_time = time.time() - start_time
            print(f"[{datetime.now()}] Error while reading file {filepath}: {e}")
            if elapsed_time >= max_wait_time:
                raise TimeoutError(f"File {filepath} is not ready after {max_wait_time} seconds.")
            time.sleep(0.5)

def get_urls_from_file(filepath: str) -> tuple:
    urls = []
    total_messages = 0
    try:
        wait_for_file(filepath)
        if filepath.endswith(".gz"):
            with gzip.open(filepath, 'rt', encoding='utf-8') as f:
                data = json.load(f)
                total_messages = len(data)
                for item in data:
                    if "urls" in item:
                        urls.extend(item["urls"])
        else:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                total_messages = len(data)
                for item in data:
                    if "urls" in item:
                        urls.extend(item["urls"])        
    except Exception as e:
        print(f"Error processing file {filepath}: {e}")
    return urls, total_messages

async def process_urls(short_domains: Set[str], total_urls: list) -> tuple:
    unshortened_urls = []
    unshortened_count = 0

    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in total_urls:
            if is_short_url(url, short_domains):
                tasks.append(unshorten_url(session, url))
            else:
                unshortened_urls.append(url)

        results = await asyncio.gather(*tasks)
        for result in results:
            if result:
                unshortened_urls.append(result)
                unshortened_count += 1

    return unshortened_count, unshortened_urls

async def process_file_pair(processed_file: str, corresponding_file: str, short_domains: Set[str]):
    try:
        processed_file_path = os.path.join(processed_file, processed_file)
        corresponding_file_path = os.path.join(corresponding_file, corresponding_file)
        urls_from_processed, messages_processed = get_urls_from_file(processed_file_path)
        urls_from_corresponding, total_messages = get_urls_from_file(corresponding_file_path)
        total_urls = urls_from_processed + urls_from_corresponding
        unshortened_count, unshortened_urls = await process_urls(short_domains, total_urls)

        stats = {
            'total_messages': total_messages,
            'total_links': len(total_urls),
            'above_60': 0,
            'below_60': 0
        }

        for long_url in total_urls:
            domain = extract_domain(long_url)
            if domain:
                score = domain_scores.get(domain, -1)
                if score >= 60:
                    stats['above_60'] += 1
                elif score >= 0:
                    stats['below_60'] += 1
        print(f"Total Messages: {stats['total_messages']}")
        print(f"Total Links: {stats['total_links']}")
        print(f"Total Above 60: {stats['above_60']}")
        print(f"Total Below 60: {stats['below_60']}")
        file_path = "total_urls.txt"
        with open(file_path, "w") as f:
            for url in total_urls:
                f.write(f"{url}\n")
    except Exception as e:
        print(f"Error processing file pair {processed_file} and {corresponding_file}: {e}")

async def main():
    await load_domain_scores()
    shorturl_services = load_short_url_domains("shorturl-services-list.csv")
    destination_path = re.sub(r'(?<=Payload/)[^/]+(?=/)', 'processed_reposts_and_likes', sys.argv[1]).replace('.gz', '')
    destination_path = re.sub(r'([^/]+)$', r'processed_\1', destination_path)
    await process_file_pair(destination_path, sys.argv[1], shorturl_services)

if __name__ == "__main__":
    asyncio.run(main())