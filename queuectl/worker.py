import subprocess
import time
from queuectl import storage

BASE_BACKOFF = 2  # exponential backoff base (2^attempts)

def process_job(job):
    job_id, command, state, attempts, max_retries, created_at, updated_at = job
    print(f"Processing job {job_id}...")

    try:
        result = subprocess.run(command, shell=True)
        if result.returncode == 0:
            storage.update_job_state(job_id, 'completed', attempts)
            print(f"✅ Job {job_id} completed.")
        else:
            raise Exception("Command failed")
    except Exception:
        attempts += 1
        if attempts > max_retries:
            storage.update_job_state(job_id, 'dead', attempts)
            print(f"💀 Job {job_id} moved to DLQ.")
        else:
            delay = BASE_BACKOFF ** attempts
            print(f"⚠️ Retrying job {job_id} in {delay}s...")
            time.sleep(delay)
            storage.update_job_state(job_id, 'pending', attempts)

def start_worker():
    print("👷 Worker started...")
    while True:
        jobs = storage.get_jobs_by_state('pending')
        if not jobs:
            time.sleep(2)
            continue
        for job in jobs:
            storage.update_job_state(job[0], 'processing', job[3])
            process_job(job)
