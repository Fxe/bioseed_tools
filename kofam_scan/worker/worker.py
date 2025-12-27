from celery import Celery
from kombu import Exchange, Queue
import subprocess
import os
import shutil
import json


USAGE = """
root@57ce091ea1fe:/app# /opt/kofam_scan/exec_annotation
Error: Specify a query file
Usage: exec_annotation [options] <query>
  <query>                    FASTA formatted query sequence file
  -o <file>                  File to output the result  [stdout]
  -p, --profile <path>       Profile HMM database
  -k, --ko-list <file>       KO information file
  --cpu <num>                Number of CPU to use  [1]
  -c, --config <file>        Config file
  --tmp-dir <dir>            Temporary directory  [./tmp]
  -E, --e-value <e_value>    Largest E-value required of the hits
  -T, --threshold-scale <scale>
                             The score thresholds will be multiplied by this value
  -f, --format <format>      Format of the output [detail]
      detail:          Detail for each hits (including hits below threshold)
      detail-tsv:      Tab separeted values for detail format
      mapper:          KEGG Mapper compatible format
      mapper-one-line: Similar to mapper, but all hit KOs are listed in one line
  --[no-]report-unannotated  Sequence name will be shown even if no KOs are assigned
                             Default is true when format=mapper or mapper-all,
                             false when format=detail
  --create-alignment         Create domain annotation files for each sequence
                             They will be located in the tmp directory
                             Incompatible with -r
  -r, --reannotate           Skip hmmsearch
                             Incompatible with --create-alignment
  --keep-tabular             Neither create tabular.txt nor delete K number files
                             By default, all K number files will be combined into
                             a tabular.txt and delete them
  --keep-output              Neither create output.txt nor delete K number files
                             By default, all K number files will be combined into
                             a output.txt and delete them
                             Must be with --create-alignment
  -h, --help                 Show this message and exit

"""


TIMEOUT = 3600 * 10  # 1 hour X
QUEUE = 'kofam_scan'
EXEC_PATH = '/local/kofam_scan/run'

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


"""
rule kofamscan:
    input:
        TEMP_RESULTS_DIR / "prodigal/{sample}.faa"
    output:
        TEMP_RESULTS_DIR / "kofamscan/{sample}.txt"
    params:
        profiles = DATA_DIR / "kofamscan/profiles",
        ko_list = DATA_DIR / "kofamscan/ko_list"
    threads: 2
    shell:
        "exec_annotation -f mapper --profile {params.profiles} --ko-list {params.ko_list} -o {output} {input} --tmp_dir {SCRATCH_DIR}/tmp/{wildcards.sample} --cpu {threads} && rm -rf {SCRATCH_DIR}/tmp/{wildcards.sample}"
exec_annotation -f mapper --profile 
/opt/kofam_scan/exec_annotation -f mapper --profile profiles --ko-list ko_list_test -o test_output test.faa
"""


@app.task(bind=True, name=f'{QUEUE}.exec_annotation')
def run(self, threads, name, profiles, proteins, param_f='mapper'):
    """
    Simple calculator task.
    """

    #self.update_state(state='PROGRESS', meta={'status': 'Running test command...'})

    job_dir = f'{EXEC_PATH}/{name}/'
    if os.path.exists(job_dir):
        raise ValueError(f'Invalid job dir: {job_dir} exists')
    os.makedirs(job_dir)
    # Build cmd
    cmd = [
        '/opt/kofam_scan/exec_annotation',
        '--cpu', str(threads),
        '-p', f'/db/profiles/{profiles}',
        '-k', f'/db/profiles/{profiles}.txt',
        '-f', param_f,
        '--tmp-dir', f'{job_dir}/',
        '-o', f'{job_dir}/output',
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


@app.task(bind=True, name=f'{QUEUE}.delete')
def delete(self, name):

    job_dir = f'{EXEC_PATH}/{name}/'
    if not os.path.exists(job_dir):
        return False

    shutil.rmtree(job_dir)

    return True


@app.task(bind=True, name=f'{QUEUE}.get_result')
def get_result(self, name):

    job_dir = f'{EXEC_PATH}/{name}/'
    if not os.path.exists(job_dir):
        raise ValueError(f'Invalid job dir: {job_dir} does not exist')

    filename_json = f'{job_dir}/output'
    if not os.path.exists(filename_json):
        raise ValueError(f'Result not found')

    with open(filename_json, 'r') as fh:
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
