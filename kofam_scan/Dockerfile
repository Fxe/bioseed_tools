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


RUN wget -qO- https://astral.sh/uv/install.sh | sh

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
COPY ./src /opt

COPY ./src/worker/worker.py /worker/worker.py
COPY ./src/worker/run_worker.sh /worker/run_worker.sh

RUN chmod a+x /worker/run_worker.sh

RUN /bin/bash -c "source /worker/env/bin/activate && uv pip install celery[redis]"

# (Optional) install Python deps
# RUN pip install --no-cache-dir -r requirements.txt

#ENTRYPOINT ["/worker/run_worker.sh"]
ENTRYPOINT ["/bin/bash"]
