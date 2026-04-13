import requests
import time
from multiprocessing import Pool, cpu_count

# Configuration
BASE_URL = "http://72.60.221.150:8080"
STUDENT_ID = "MDS202540"  # IMPORTANT: Replace with your actual student ID

def get_secret_key(student_id):
    """Helper function to fetch the dynamic SHA-256 session key."""
    for attempt in range(5):
        try:
            resp = requests.post(f"{BASE_URL}/login", json={"student_id": student_id})
            if resp.status_code == 200:
                return resp.json().get("secret_key")
            elif resp.status_code == 429:
                time.sleep(1) # Backoff if throttled
        except Exception as e:
            time.sleep(1)
    raise ConnectionError("Failed to obtain secret key after multiple attempts.")

def get_publication_title(student_id, filename):
    """
    Fetches the publication title. Note: In the Map-Reduce workflow below,
    we handle the API calls directly inside the mapper for better performance 
    so we don't have to authenticate 1000 individual times.
    """
    key = get_secret_key(student_id)
    while True:
        resp = requests.post(f"{BASE_URL}/lookup", json={"secret_key": key, "filename": filename})
        if resp.status_code == 200:
            return resp.json().get("title", "")
        elif resp.status_code == 429:
            time.sleep(0.5) # Handle 429 Too Many Requests
        else:
            return None

def verify_top_10(student_id, top_10_list):
    """Logs in and submits the final top 10 list for grading."""
    print("Submitting Top 10 for verification...")
    key = get_secret_key(student_id)
    
    resp = requests.post(f"{BASE_URL}/verify", json={"secret_key": key, "top_10": top_10_list})
    if resp.status_code == 200:
        data = resp.json()
        print("\n--- VERIFICATION RESULTS ---")
        print(f"Score: {data.get('score')} / {data.get('total')}")
        print(f"Correct: {data.get('correct')}")
        print(f"Message: {data.get('message')}")
    else:
        print(f"Verification failed with status {resp.status_code}: {resp.text}")

def mapper(filename_chunk):
    """
    Map Phase: Retrieves titles for a chunk of files and calculates local frequencies.
    Authenticates once per worker to minimize /login payload limits.
    """
    key = get_secret_key(STUDENT_ID)
    local_counts = {}
    
    for filename in filename_chunk:
        success = False
        while not success:
            try:
                resp = requests.post(f"{BASE_URL}/lookup", json={"secret_key": key, "filename": filename})
                if resp.status_code == 200:
                    title = resp.json().get("title", "")
                    if title:
                        # Extract the first word
                        words = title.strip().split()
                        if words:
                            first_word = words[0]
                            local_counts[first_word] = local_counts.get(first_word, 0) + 1
                    success = True
                elif resp.status_code == 429:
                    time.sleep(0.5) # Respect the 100 requests/sec limit
                else:
                    success = True # Break loop on 404/500 errors to avoid infinite loops
            except Exception:
                time.sleep(0.5)
                
    return local_counts

if __name__ == "__main__":
    start_time = time.time()
    print("Starting Map-Reduce Job...")
    
    # 1. Divide filenames (pub_0.txt to pub_999.txt) into chunks
    total_files = 1000
    all_filenames = [f"pub_{i}.txt" for i in range(total_files)]
    
    # Determine chunk size based on available CPU cores
    num_workers = cpu_count() * 2 # Good heuristic for I/O bound network tasks
    chunk_size = (total_files // num_workers) + 1
    chunks = [all_filenames[i:i + chunk_size] for i in range(0, total_files, chunk_size)]
    
    # 2. Map Phase
    print(f"Executing Map phase with {len(chunks)} workers...")
    with Pool(processes=num_workers) as pool:
        map_results = pool.map(mapper, chunks)
        
    # 3. Reduce Phase
    print("Executing Reduce phase...")
    global_counts = {}
    for local_counts in map_results:
        for word, count in local_counts.items():
            global_counts[word] = global_counts.get(word, 0) + count
            
    # 4. Identify Top 10
    # Sort dictionary by value (count) in descending order
    sorted_words = sorted(global_counts.items(), key=lambda item: item[1], reverse=True)
    top_10 = [word for word, count in sorted_words[:10]]
    
    print(f"Map-Reduce finished in {round(time.time() - start_time, 2)} seconds.")
    print(f"Top 10 Words Found: {top_10}")
    
    # 5. Verify
    if top_10:
        verify_top_10(STUDENT_ID, top_10)
    else:
        print("Compute the top 10 words first!")