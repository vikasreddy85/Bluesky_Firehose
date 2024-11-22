from itertools import islice
from datetime import datetime
import sys, os, json, gzip, asyncio, aiohttp, aiofiles

BASE_FOLDER = "./Payload"
MAX_CONCURRENT_REQUESTS = 2000
sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
file_creation_event = asyncio.Event()

async def fetch_data_from_api(session, dids, batch_size=1000, delay_between_batches=0.1):
    async def fetch_single_did(did):
        try:
            url = f"https://public.api.bsky.app/xrpc/app.bsky.feed.getAuthorFeed?actor={did}"
            async with session.get(url) as response:
                if response.status == 200:
                    return did, await response.json()
                else:
                    print(f"{datetime.now()} - Error: Unexpected response status {response.status}")
                    return did, None
        except Exception as e:
            print(f"{datetime.now()} - Error fetching data for DID {did}: {e}")
            return did, None

    def batched(iterable, size):
        iterator = iter(iterable)
        return iter(lambda: list(islice(iterator, size)), [])

    results = {}
    for batch_dids in batched(dids, batch_size):
        batch_tasks = [fetch_single_did(did) for did in batch_dids]            
        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=False)
        for did, data in batch_results:
            if data is not None:
                results[did] = data            
        await asyncio.sleep(delay_between_batches)
    return results

def process_uri(uri):
    try:
        return uri.split("/")[-3]
    except IndexError:
        raise ValueError(f"Invalid URI format: {uri}")

def extract_uris(data):
    uri_list = []
    url = data.get('embed', {}).get('external', {}).get('uri')
    if url:
        uri_list.append(url)
    return list(set(uri_list))

def find_matching_payload(feed, target_cid):
    for item in feed:
        if item.get('post', {}).get('cid') == target_cid:
            return item
    return None

async def save_processed_file(new_file_path, existing_payloads):
    try:
        os.makedirs(os.path.dirname(new_file_path), exist_ok=True)
        async with aiofiles.open(new_file_path, "w", encoding="utf-8") as f:
            await f.write(json.dumps(existing_payloads, indent=3, ensure_ascii=False))
        file_creation_event.set()
        
    except Exception as e:
        print(f"Error saving processed file {new_file_path}: {e}")

async def process_files(file_path):
    async with aiohttp.ClientSession() as session:
        try:
            dids = set()
            file_data = []
            
            with gzip.open(file_path, "rt", encoding="utf-8") as f:
                entire_data = json.load(f)
                for data in entire_data:
                    commit = data.get("commit", {})
                    collection = commit.get("collection")
                    if collection in {"app.bsky.feed.like", "app.bsky.feed.repost"}:
                        record = commit.get("record", {})
                        subject_uri = record.get("subject", {}).get("uri")
                        target_cid = record.get("subject", {}).get("cid")
                        if subject_uri and target_cid:
                            did = process_uri(subject_uri)
                            dids.add(did)
                            file_data.append((data, did, target_cid))

            feeds = await fetch_data_from_api(session, list(dids))
            records = []
            for original_payload, did, target_cid in file_data:
                feed = feeds.get(did, {}).get('feed', [])
                matching_payload = find_matching_payload(feed, target_cid)
                if matching_payload:
                    urls = extract_uris(matching_payload['post']['record'])
                    updated_payload = {
                        "originalDid": process_uri(original_payload['commit']['record']['subject']['uri']),
                        "newDid": original_payload['did'],
                        "newType": original_payload['commit']['collection'],
                        "text": matching_payload['post']['record']['text'],
                        "urls": urls,
                        "repostCount": matching_payload['post']['repostCount'],
                        "likeCount": matching_payload['post']['likeCount'],
                        "quoteCount": matching_payload['post']['quoteCount'],
                        "newCreatedAt": original_payload['timestamp'],
                    }
                    records.append(updated_payload)
            
            if records:
                processed_folder = os.path.join(BASE_FOLDER, "processed_reposts_and_likes")
                os.makedirs(processed_folder, exist_ok=True)
                original_name = os.path.basename(file_path).replace(".gz", "")
                new_file_name = f"processed_{original_name}"
                new_file_path = os.path.join(processed_folder, new_file_name)
                await save_processed_file(new_file_path, records)
                print(f"Successfully saved {len(records)} records to {new_file_path}")

        except Exception as e:
            print(f"Error processing file {file_path}: {e}")

def main():
    if len(sys.argv) < 2:
        print("Provide the directory containing .gz files as an argument.")
        sys.exit(1)
    asyncio.run(process_files(sys.argv[1]))
    file_creation_event.clear()

if __name__ == "__main__":
    main()

