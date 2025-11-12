import sqlite3
from datetime import datetime

DB_PATH = "jobs.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            command TEXT,
            state TEXT,
            attempts INTEGER,
            max_retries INTEGER,
            created_at TEXT,
            updated_at TEXT
        )
    ''')
    conn.commit()
    conn.close()

def add_job(job):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('INSERT INTO jobs VALUES (?,?,?,?,?,?,?)', (
        job['id'], job['command'], job['state'],
        job['attempts'], job['max_retries'],
        job['created_at'], job['updated_at']
    ))
    conn.commit()
    conn.close()

def update_job_state(job_id, state, attempts=None):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        UPDATE jobs SET state=?, attempts=?, updated_at=?
        WHERE id=?
    ''', (state, attempts, datetime.utcnow().isoformat(), job_id))
    conn.commit()
    conn.close()

def get_jobs_by_state(state):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT * FROM jobs WHERE state=?', (state,))
    rows = c.fetchall()
    conn.close()
    return rows
