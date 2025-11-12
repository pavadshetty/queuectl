import click
import json
from datetime import datetime
from queuectl import storage, worker

@click.group()
def cli():
    """QueueCTL CLI tool"""
    storage.init_db()

@cli.command()
@click.argument('job_json')
def enqueue(job_json):
    """Add a new job to the queue"""
    job = json.loads(job_json)
    job.setdefault('state', 'pending')
    job.setdefault('attempts', 0)
    job.setdefault('max_retries', 3)
    now = datetime.utcnow().isoformat()
    job.setdefault('created_at', now)
    job.setdefault('updated_at', now)
    storage.add_job(job)
    click.echo(f"📦 Enqueued job {job['id']}")

@cli.command(name="worker-start")  # Use hyphen in CLI
def worker_start():
    """Start worker process"""
    worker.start_worker()

@cli.command()
@click.option('--state', default='pending')
def list(state):
    """List jobs by state"""
    jobs = storage.get_jobs_by_state(state)
    click.echo(jobs)

@cli.command()
def status():
    """Show summary of job states"""
    from queuectl import storage
    import sqlite3
    conn = sqlite3.connect(storage.DB_PATH)
    c = conn.cursor()
    c.execute("SELECT state, COUNT(*) FROM jobs GROUP BY state")
    rows = c.fetchall()
    conn.close()
    if not rows:
        print("📊 No jobs found.")
        return
    print("📊 Job Status:")
    for state, count in rows:
        print(f"  {state}: {count}")

@cli.group()
def dlq():
    """Dead Letter Queue operations"""
    pass

@dlq.command("list")
def dlq_list():
    """List dead jobs"""
    from queuectl import storage
    jobs = storage.get_jobs_by_state('dead')
    if not jobs:
        click.echo("🧺 No jobs in DLQ.")
        return
    for job in jobs:
        click.echo(f"{job[0]} - {job[1]} - attempts={job[3]}")

@dlq.command("retry")
@click.argument("job_id")
def dlq_retry(job_id):
    """Retry a job from the DLQ"""
    import sqlite3
    from queuectl import storage
    conn = sqlite3.connect(storage.DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE jobs SET state='pending', attempts=0 WHERE id=?", (job_id,))
    conn.commit()
    conn.close()
    click.echo(f"🔁 Job {job_id} moved back to pending queue.")

if __name__ == '__main__':
    cli()
