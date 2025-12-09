FROM brinkmanlab/psortb_commandline:1.0.2

# setup python env for worker
RUN wget -qO- https://astral.sh/uv/install.sh | sh
RUN mkdir /worker
RUN /root/.local/bin/uv venv --python 3.13 /worker/env
RUN /root/.local/bin/uv pip install --python /worker/env "celery[redis]"

COPY ./worker/worker.py /worker/worker.py
COPY ./worker/run_worker.sh /worker/run_worker.sh

RUN chmod a+x /worker/run_worker.sh

# add a modified version that allows outputfile specification
COPY ./psortx /usr/local/psortb/bin/psortx

ENTRYPOINT ["/worker/run_worker.sh"]
#ENTRYPOINT ["/bin/bash"]