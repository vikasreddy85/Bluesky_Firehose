import json, asyncio, websockets, os, logging, gzip, time
from datetime import datetime
from collections import deque

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
current_day_folder = None

def ensure_daily_folder():
    global current_day_folder
    today = datetime.now().strftime('%Y-%m-%d')
    new_folder_path = f"./Payload/{today}"
    if current_day_folder != new_folder_path:
        if not os.path.exists(new_folder_path):
            os.makedirs(new_folder_path)
        current_day_folder = new_folder_path
    return current_day_folder

def extract_uris(data):
    uri_list = []
    if isinstance(data, dict):
        url = data.get('embed', {}).get('external', {}).get('uri')
        if url:
            uri_list.append(url)
    elif isinstance(data, list):
        for item in data:
            uri_list.extend(extract_uris(item))
    return list(set(uri_list))

async def connect_and_collect(queue, url="wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post&wantedCollections=app.bsky.feed.like&wantedCollections=app.bsky.feed.repost"):
    while True:
        try:
            async with websockets.connect(url) as websocket:
                while True:
                    try:
                        message = await websocket.recv()
                        try:
                            message_json = json.loads(message)
                            if message_json.get('commit', {}).get('collection') == "app.bsky.feed.post":
                                urls = extract_uris(message_json.get('commit', {}).get('record', {}))
                                message_json["urls"] = urls
                            else:
                                message_json["urls"] = []
                            message_json['timestamp'] = datetime.now().isoformat()
                            await queue.put(message_json)
                        except json.JSONDecodeError:
                            logging.warning("Failed to decode message JSON")
                    except websockets.ConnectionClosed:
                        logging.warning("WebSocket connection closed")
                        break
        except Exception as e:
            logging.error(f"WebSocket error: {e}")
            await asyncio.sleep(0.1)

async def compress_periodically(queue, interval=3600):
    while True:
        folder_path = ensure_daily_folder()
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        filename = f"{timestamp}.json.gz"
        full_path = os.path.join(folder_path, filename)        
        buffer = deque()
        start_time = time.time()

        while (time.time() - start_time) < interval:
            try:
                message = await asyncio.wait_for(queue.get(), timeout=interval - (time.time() - start_time))
                buffer.append(message)
            except asyncio.TimeoutError:
                continue

        if buffer:
            buffer_json = json.dumps(list(buffer), indent=3, ensure_ascii=False, separators=(',', ':'))
            try:
                with gzip.open(full_path, 'wt', encoding='utf-8') as f:
                    f.write(buffer_json)
            except Exception as e:
                logging.error(f"Compression error: {e}")
        while not queue.empty():
            queue.get_nowait()

async def main():
    queue = asyncio.Queue()
    await asyncio.gather(connect_and_collect(queue), compress_periodically(queue))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Script terminated by user")
