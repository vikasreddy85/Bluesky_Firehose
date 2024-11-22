import json
import asyncio
import websockets
import os
import gzip
import logging
from datetime import datetime
from threading import Lock

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

current_day_folder = None
messages_buffer = []
buffer_lock = Lock()

def ensure_daily_folder():
    global current_day_folder
    today = datetime.now().strftime('%Y-%m-%d')
    new_folder_path = f"./Payload/{today}"
    if current_day_folder != new_folder_path:
        if not os.path.exists(new_folder_path):
            os.makedirs(new_folder_path)
        current_day_folder = new_folder_path
    return current_day_folder

def write_messages_to_file():
    global messages_buffer
    with buffer_lock:
        if not messages_buffer:
            return None
        folder_path = ensure_daily_folder()
        filename = f"{folder_path}/{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}.json"
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(messages_buffer, f, indent=3, ensure_ascii=False, separators=(',', ':'))
            messages_buffer.clear()
            return filename
        except Exception as e:
            logging.error(f"Error writing messages to file: {e}")
            return None

def compress_file(filename):
    if not filename or not os.path.exists(filename):
        return
    
    try:
        with open(filename, 'rb') as f_in:
            compressed_filename = f"{filename}.gz"
            with gzip.open(compressed_filename, 'wb') as f_out:
                f_out.writelines(f_in)
        os.remove(filename)
        return compressed_filename
    except Exception as e:
        logging.error(f"Error compressing file {filename}: {e}")
        return None

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

async def connect_and_collect(url="wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post&wantedCollections=app.bsky.feed.like&wantedCollections=app.bsky.feed.repost"):
    global messages_buffer

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
                            with buffer_lock:
                                messages_buffer.append(message_json)
                        except json.JSONDecodeError:
                            pass
                    except websockets.ConnectionClosed:
                        break
        except Exception as e:
            logging.error(f"Error in WebSocket connection: {e}")
            await asyncio.sleep(5)

async def write_and_compress_every_interval(interval=3600):
    while True:
        await asyncio.sleep(interval)
        try:
            filename = write_messages_to_file()
            if filename:
                compress_file(filename)
        except Exception as e:
            logging.error(f"Error during write and compress operation: {e}")

async def main():
    await asyncio.gather(
        connect_and_collect(),
        write_and_compress_every_interval(),
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Script terminated by user")
