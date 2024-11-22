import os
import time
import gzip
import json
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import subprocess
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
import threading

class FileEventHandler(FileSystemEventHandler):
    def __init__(self, queue):
        self.queue = queue
        self.preprocess_lock = threading.Lock()

    def on_created(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith(('.gz')):
            print(f"Detected new file: {event.src_path}")
            if self.wait_for_file(event.src_path):
                self.queue.put(event.src_path)

    def wait_for_file(self, file_path, max_wait=10, check_interval=1):
        last_size = -1
        elapsed_time = 0

        while elapsed_time < max_wait:
            try:
                current_size = os.path.getsize(file_path)
                if current_size == last_size:
                    try:
                        if file_path.endswith('.gz'):
                            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                                json.load(f)
                        return True
                    except (OSError, json.JSONDecodeError):
                        pass
                last_size = current_size
            except FileNotFoundError:
                pass

            time.sleep(check_interval)
            elapsed_time += check_interval

        print(f"Warning: File {file_path} might not be fully written after {max_wait} seconds.")
        return False

def process_file(file_path):
    try:
        print(f"Processing file: {file_path}")
        subprocess.run(['python3', 'data_processing.py', file_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error processing file {file_path}: {e}")
    except Exception as e:
        print(f"Unexpected error with file {file_path}: {e}")

def run_preprocess(file_path, preprocess_lock):
    with preprocess_lock:
        print(f"Running preprocess for: {file_path}")
        subprocess.run(['python3', 'preprocess.py', file_path], check=True)

def start_websocket():
    print("Starting websocket.py subprocess")
    return subprocess.Popen(['python3', 'websocket.py'])

def orchestrate():
    file_queue = Queue()
    event_handler = FileEventHandler(file_queue)
    observer = Observer()
    observer.schedule(event_handler, path='./Payload', recursive=True)
    observer.start()

    websocket_process = start_websocket()

    with ThreadPoolExecutor(max_workers=4) as executor:
        try:
            while True:
                file_path = file_queue.get()
                if file_path:
                    run_preprocess(file_path, event_handler.preprocess_lock)
                    executor.submit(process_file, file_path)
        except KeyboardInterrupt:
            observer.stop()

    observer.join()

    if websocket_process.poll() is None:
        print("Stopping websocket.py subprocess")
        websocket_process.terminate()

if __name__ == '__main__':
    orchestrate()
