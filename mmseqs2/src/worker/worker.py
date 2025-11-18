from celery import Celery
import subprocess
import os
import shutil
import json


BAKTA_OUTPUT_FOLDER_NAME = 'results'
TIMEOUT = 3600 * 10  # 1 hour X

# Configure Celery
app = Celery(
    'mmseqs2',
    broker=os.getenv('CELERY_BROKER_URL', 'redis://bioseed_redis:6379/10'),
    backend=os.getenv('CELERY_RESULT_BACKEND', 'redis://bioseed_redis:6379/10')
)

# Configure Celery settings
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=TIMEOUT,
    worker_prefetch_multiplier=1,
)


@app.task(bind=True, name='mmseqs.easy_cluster')
def mmseqs_easy_cluster(self, threads, name, proteins):
    """
    Simple calculator task.
    """

    #self.update_state(state='PROGRESS', meta={'status': 'Running test command...'})

    job_dir = f'/host/run/{name}'
    if os.path.exists(job_dir):
        raise ValueError(f'Invalid job dir: {job_dir} exists')
    os.makedirs(f'/host/run/{name}')

    # Build cmd
    cmd = [
        'bakta_proteins',
        '--threads', str(threads),
        '--db', '/host/db',
        '--output', f'{job_dir}/{BAKTA_OUTPUT_FOLDER_NAME}',
        f'{job_dir}/input.faa',
    ]

    with open(f'{job_dir}/input.faa', 'w') as fh:
        for protein_id, protein_seq in proteins.items():
            fh.write(f'>{protein_id}\n')
            fh.write(f'{protein_seq}\n')

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=TIMEOUT
    )

    return {
        'cmd': ' '.join(cmd),
        'status': 'success' if result.returncode == 0 else 'error',
        'output': result.stdout.strip() if result.stdout else '',
        'stderr': result.stderr.strip() if result.stderr else '',
        'return_code': result.returncode
    }


@app.task(bind=True, name='bakta.delete')
def bakta_delete(self, name):

    job_dir = f'/host/run/{name}'
    if not os.path.exists(job_dir):
        raise ValueError(f'Invalid job dir: {job_dir} does not exist')

    shutil.rmtree(job_dir)

    return True


@app.task(bind=True, name='bakta.get_result')
def bakta_get_result(self, name):

    job_dir = f'/host/run/{name}'
    if not os.path.exists(job_dir):
        raise ValueError(f'Invalid job dir: {job_dir} does not exist')

    filename_json = f'{job_dir}/{BAKTA_OUTPUT_FOLDER_NAME}/input.json'
    if not os.path.exists(filename_json):
        raise ValueError(f'Result not found')

    with open(filename_json, 'r') as fh:
        return json.load(fh)


def main():
    """
    Start the Celery worker.
    """
    print("Starting Celery Worker...")
    print(f"Broker: {app.conf.broker_url}")
    print(f"Backend: {app.conf.result_backend}")

    # Start worker
    app.worker_main([
        'worker',
        '--loglevel=info',
        '--concurrency=2',
        f'--hostname=batka@%h'
    ])


if __name__ == "__main__":
    main()
