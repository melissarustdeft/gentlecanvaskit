"""interpolator_dcb3a3 - Concurrent worker pool."""
import threading, queue, time, json
POOL_NAME = "interpolator_dcb3a3"
def worker(q: queue.Queue, results: list, lock: threading.Lock):
    while True:
        try:
            item = q.get(timeout=0.5)
            result = {"item": item, "thread": threading.current_thread().name, "pool": POOL_NAME}
            with lock: results.append(result)
            q.task_done()
        except queue.Empty: break
def run_pool(items: list, num_workers: int = 3) -> list:
    q, results, lock = queue.Queue(), [], threading.Lock()
    for item in items: q.put(item)
    threads = [threading.Thread(target=worker, args=(q, results, lock), name=f"{POOL_NAME}-w{i}") for i in range(num_workers)]
    for t in threads: t.start()
    q.join()
    for t in threads: t.join()
    return results
def main():
    print(f"[{POOL_NAME}] Starting worker pool...")
    results = run_pool(list(range(10)))
    print(f"[{POOL_NAME}] Processed {len(results)} items")
    print(json.dumps(results[:3], indent=2))
if __name__ == "__main__":
    main()
