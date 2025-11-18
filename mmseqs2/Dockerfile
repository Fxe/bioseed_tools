FROM python:3.13-slim

RUN wget -qO- https://astral.sh/uv/install.sh | sh

RUN mkdir /worker

RUN uv venv --python 3.13 /worker/env

COPY ./src/worker/worker.py /worker/worker.py
COPY ./src/worker/run_worker.sh /worker/run_worker.sh

RUN chmod a+x /worker/run_worker.sh

RUN /bin/bash -c "source /worker/env/bin/activate && uv pip install celery[redis]"

#ENTRYPOINT ["/worker/run_worker.sh"]
ENTRYPOINT ["/bin/bash"]