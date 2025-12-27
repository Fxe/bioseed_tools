from celery import Celery
from kombu import Exchange, Queue
import subprocess
import os
import shutil
import json


BAKTA_OUTPUT_FOLDER_NAME = 'results'
TIMEOUT = 3600 * 10  # 1 hour X
QUEUE = 'bakta'
EXEC_PATH = '/local/bakta/run'

# Configure Celery
app = Celery(
    QUEUE,
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

    # IMPORTANT PART:
    task_default_queue=QUEUE,
    task_default_exchange=QUEUE,
    task_default_routing_key=QUEUE,

    task_queues=(
        Queue(QUEUE, Exchange(QUEUE), routing_key=QUEUE),
    ),

    task_routes={
        f'{QUEUE}.*': {'queue': QUEUE, 'routing_key': QUEUE},
    },
)


@app.task(bind=True, name='bakta.pipeline')
def pipeline(self, args, genome, output):
    job_dir = f'{EXEC_PATH}/pipeline/{output}'
    tmp = f'{job_dir}/tmp'
    if os.path.exists(job_dir):
        raise ValueError(f'Invalid job dir: {job_dir} exists')
    os.makedirs(job_dir)
    os.makedirs(tmp)
    cmd = [
        'bakta',
        args,
        '--db', '/host/db',
        '--output', job_dir,
        '--tmp-dir', tmp,
        genome
    ]


@app.task(bind=True, name='bakta.bakta_proteins')
def bakta_bakta_proteins(self, threads, name, proteins, use_index=True, index_results=False):
    """
    Simple calculator task.
    """

    #self.update_state(state='PROGRESS', meta={'status': 'Running test command...'})

    if use_index:
        from pymongo import MongoClient
        database = MongoClient('poplar.cels.anl.gov', 27017)['database']
        col_bakta = database['seq_protein_bakta']
        for k in proteins:
            _doc = col_bakta.find_one({'_id': k})
            if _doc:
                pass

    job_dir = f'{EXEC_PATH}/proteins/{name}'
    if os.path.exists(job_dir):
        raise ValueError(f'Invalid job dir: {job_dir} exists')
    os.makedirs(job_dir)

    with open(f'{job_dir}/input.faa', 'w') as fh:
        for protein_id, protein_seq in proteins.items():
            fh.write(f'>{protein_id}\n')
            fh.write(f'{protein_seq}\n')

    # Build cmd
    cmd = [
        'bakta_proteins',
        '--threads', str(threads),
        '--db', '/host/db',
        '--output', f'{job_dir}/{BAKTA_OUTPUT_FOLDER_NAME}',
        f'{job_dir}/input.faa',
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=TIMEOUT
    )

    if index_results:
        from pymongo import MongoClient
        client = MongoClient('poplar.cels.anl.gov', 27017)
        database = client['database']
        col_bakta = database['seq_protein_bakta']

        with open(path_annotation_bakta, 'r') as fh:
            annotation = json.load(fh)
            for o in annotation['features']:
                _doc = col_bakta.find_one({'_id': o['id']})
                if _doc is None:
                    _doc = {}
                    for k, v in o.items():
                        if k == 'id':
                            _doc['_id'] = v
                        else:
                            _doc[k] = v
                    col_bakta.insert_one(_doc)

    return {
        'cmd': ' '.join(cmd),
        'status': 'success' if result.returncode == 0 else 'error',
        'output': result.stdout.strip() if result.stdout else '',
        'stderr': result.stderr.strip() if result.stderr else '',
        'return_code': result.returncode
    }


@app.task(bind=True, name='bakta.delete')
def bakta_delete(self, name):

    job_dir = f'{EXEC_PATH}/{name}/'
    if not os.path.exists(job_dir):
        return False

    shutil.rmtree(job_dir)

    return True


@app.task(bind=True, name='bakta.get_result')
def bakta_get_result(self, name):

    job_dir = f'{EXEC_PATH}/{name}/'
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
        f'--queues={QUEUE}',
        f'--hostname={QUEUE}@%h'
    ])


if __name__ == "__main__":
    main()
