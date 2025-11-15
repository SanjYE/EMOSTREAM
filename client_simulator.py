import requests
import random
import time
import threading
import os

FLASK_ENDPOINT = 'http://localhost:5000/emoji'

def load_emojis(file_path):
    if not os.path.exists(file_path):
        print(f"Emoji file '{file_path}' not found.")
        exit(1)
    with open(file_path, 'r', encoding='utf-8') as f:
        emojis = [line.strip() for line in f if line.strip()]
    if not emojis:
        print("No emojis found in the file.")
        exit(1)
    return emojis

EMOJI_FILE_PATH = 'emojis.txt'
emojis = load_emojis(EMOJI_FILE_PATH)
NUM_THREADS = 50

REQUESTS_PER_THREAD = 1000

def send_requests(thread_id):
    for i in range(REQUESTS_PER_THREAD):
        data = {
            "user_id": f"user_{random.randint(1, 1000)}",
            "emoji_type": random.choice(emojis),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
        try:
            response = requests.post(FLASK_ENDPOINT, json=data)
            if response.status_code != 200:
                print(f"Thread {thread_id}: Received status code {response.status_code}")
        except Exception as e:
            print(f"Thread {thread_id}: Exception occurred - {e}")
        time.sleep(0.01)

def main():
    threads = []
    for thread_id in range(NUM_THREADS):
        thread = threading.Thread(target=send_requests, args=(thread_id,))
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()
    
    print("All threads have completed sending requests.")

if __name__ == "__main__":
    main()
