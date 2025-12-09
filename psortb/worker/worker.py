from celery import Celery
from kombu import Exchange, Queue
import subprocess
import os
import shutil
import json


TIMEOUT = 3600 * 10  # 1 hour X
QUEUE = 'psortb'
EXEC_PATH = '/local/psortb/run'

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


def get_protein_collection():
    from pymongo import MongoClient
    client = MongoClient('poplar.cels.anl.gov', 27017)
    database = client['database']
    col = database['seq_protein_psortb']
    return col


@app.task(bind=True, name=f'{QUEUE}.index_status')
def index_status(self):
    return {'database': None, 'collection_name': None, 'e': None}


@app.task(bind=True, name=f'{QUEUE}.run')
def run(self, name, species_type, proteins, use_index=True, index_results=False):
    """
    Simple calculator task.
    """

    #self.update_state(state='PROGRESS', meta={'status': 'Running test command...'})

    _proteins = {}

    if use_index:
        col = get_protein_collection()
        for k in proteins:
            _doc = col.find_one({'_id': k})
            if _doc:
                pass
            else:
                _proteins[k] = proteins[k]
    else:
        _proteins = proteins

    job_dir = f'{EXEC_PATH}/{name}/'
    if os.path.exists(job_dir):
        raise ValueError(f'Invalid job dir: {job_dir} exists')
    os.makedirs(f'{job_dir}')

    # create protein.faa
    with open(job_dir + '/protein.faa', 'w') as fh:
        for i, s in _proteins.items():
            fh.write(f'>{i}\n')
            fh.write(f'{s}\n')

    # Build cmd
    cmd = [
        '/usr/local/psortb/bin/psortx',
        species_type,
        '-o', 'long',
        '--outfile', f'{job_dir}/results.tsv',
        '--seq', f'{job_dir}/protein.faa'
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=TIMEOUT
    )

    """
    import sys

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1  # line-buffered
    )

    # Read stdout
    for line in process.stdout:
        print(line, end='')  # print live
        sys.stdout.flush()

    # Read stderr
    for line in process.stderr:
        print(line, end='', file=sys.stderr)
        sys.stderr.flush()

    process.wait()
    """

    if index_results:
        col = get_protein_collection()

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


@app.task(bind=True, name=f'{QUEUE}.delete')
def delete(self, name):

    job_dir = f'{EXEC_PATH}/{name}'
    if not os.path.exists(job_dir):
        return False

    shutil.rmtree(job_dir)

    return True


@app.task(bind=True, name=f'{QUEUE}.get_result')
def get_result(self, name):

    job_dir = f'{EXEC_PATH}/{name}'
    if not os.path.exists(job_dir):
        raise ValueError(f'Invalid job dir: {job_dir} does not exist')

    filename = f'{job_dir}/results.tsv'
    if not os.path.exists(filename):
        raise ValueError(f'Result not found')

    with open(filename, 'r') as fh:
        return fh.read()


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
