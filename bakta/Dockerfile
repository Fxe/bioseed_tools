FROM oschwengers/bakta:v1.11.4

# setup python env for worker
RUN wget -qO- https://astral.sh/uv/install.sh | sh
RUN mkdir /worker
RUN /root/.local/bin/uv venv --python 3.13 /worker/env
RUN /root/.local/bin/uv pip install --python /worker/env "celery[redis]"

COPY ./worker/worker.py /worker/worker.py
COPY ./worker/run_worker.sh /worker/run_worker.sh

RUN chmod a+x /worker/run_worker.sh

RUN /bin/bash -c "source /worker/env/bin/activate && uv pip install celery[redis]"

ENTRYPOINT ["/worker/run_worker.sh"]
#ENTRYPOINT ["/bin/bash"]