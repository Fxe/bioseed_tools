# Python + Ruby (>=2.4) + GNU Parallel
FROM python:3.13-slim

# Prevent interactive prompts during apt installs
ENV DEBIAN_FRONTEND=noninteractive

# Install Ruby and GNU Parallel
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential \
      ruby-full \
      parallel \
      wget

ADD ./src/hmmer.tar.gz /opt/
WORKDIR /opt/hmmer-3.4/
RUN ./configure
RUN make install

# clean up apt cache to keep the image small
RUN rm -rf /var/lib/apt/lists/*

# Verify Ruby is new enough (>= 2.4)
# (Gem::Version avoids issues like "2.10" vs float comparisons)
RUN ruby -e 'require "rubygems"; \
  min=Gem::Version.new("2.4"); \
  cur=Gem::Version.new(RUBY_VERSION); \
  abort("Ruby too old: #{RUBY_VERSION}") if cur < min' \
 && ruby --version \
 && parallel --version

# Silence GNU Parallel citation prompt for root
#RUN mkdir -p /root/.parallel && touch /root/.parallel/will-cite

# Optional: set a working directory
WORKDIR /app

# (Optional) copy your project files
COPY ./src/kofam_scan /opt/kofam_scan

RUN wget -qO- https://astral.sh/uv/install.sh | sh
RUN mkdir /worker
RUN /root/.local/bin/uv venv --python 3.13 /worker/env
RUN /root/.local/bin/uv pip install --python /worker/env "celery[redis]"

COPY ./worker/worker.py /worker/worker.py
COPY ./worker/run_worker.sh /worker/run_worker.sh

RUN chmod a+x /worker/run_worker.sh

#ENTRYPOINT ["/worker/run_worker.sh"]
ENTRYPOINT ["/bin/bash"]
