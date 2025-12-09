FROM neo4j:4.0.2-community

RUN apt-get update

# copy neo4j data
RUN rm -rf /var/lib/neo4j/data/*
ADD ./neo4j/neo4j_data.tar.gz /var/lib/neo4j/
COPY ./neo4j/neo4j.conf /var/lib/neo4j/conf

# setup Transyt tool
RUN apt-get -y install ncbi-blast+

RUN mkdir /transyt
COPY transyt.jar /transyt
COPY logback.xml /transyt
COPY ./data /transyt/data/


# setup python env for worker
RUN wget -qO- https://astral.sh/uv/install.sh | sh
RUN mkdir /worker
RUN /root/.local/bin/uv venv --python 3.13 /worker/env
RUN /root/.local/bin/uv pip install --python /worker/env "celery[redis]"

COPY ./worker/worker.py /worker/worker.py
COPY ./worker/run_worker.sh /worker/run_worker.sh

RUN chmod a+x /worker/run_worker.sh

ENTRYPOINT ["/bin/bash"]

#COPY entrypoint.sh /

#ENTRYPOINT [ "/entrypoint.sh" ]

#CMD ["tail", "-f", "/var/lib/neo4j/logs/neo4j.log"]