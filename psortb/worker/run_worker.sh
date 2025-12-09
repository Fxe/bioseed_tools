#!/bin/bash

source /worker/env/bin/activate
/root/.local/bin/uv run python /worker/worker.py
