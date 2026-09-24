set shell := ["bash", "-cu"]

jobs_dir := "/opt/spark/jobs"
stream_log := "/tmp/stream_table.log"
# Cores the long-running stream may hold, so other jobs can still get executors
stream_cores := "2"

default:
    @just --list

# Start the whole stack (Silo, Lakekeeper, Spark, Trino)
start:
    docker compose up -d

# Stop the stack (data volumes are kept)
stop:
    docker compose down

# Submit a job from ./jobs, e.g. `just submit my_job.py arg1 arg2`
submit job *args:
    @test -f "jobs/{{job}}" || { echo "jobs/{{job}} not found"; exit 1; }
    docker exec -it spark-master /opt/spark/bin/spark-submit {{jobs_dir}}/{{job}} {{args}}

# Copy PySpark jobs from etl/src/etl/ into ./jobs, replacing older copies
sync-jobs:
    @find etl/src/etl -maxdepth 1 -name '*.py' ! -name '__init__.py' -exec cp -v {} jobs/ \;

# Start the staging.core_user -> core.user stream in the background (syncs jobs first)
stream: sync-jobs
    #!/usr/bin/env bash
    set -euo pipefail
    # '[s]tream' keeps pgrep from matching its own command line
    if docker exec spark-master pgrep -f '[s]tream_table.py' >/dev/null; then
        echo "stream already running (just stream-logs / just stream-stop)"; exit 0
    fi
    docker exec -d spark-master sh -c '/opt/spark/bin/spark-submit --conf spark.cores.max={{stream_cores}} {{jobs_dir}}/stream_table.py > {{stream_log}} 2>&1'
    echo "stream started; follow it with: just stream-logs"

# Follow the background stream's log (Ctrl-C stops following, not the stream)
stream-logs:
    docker exec spark-master tail -n 50 -f {{stream_log}}

# Stop the background stream (it resumes from its checkpoint on the next `just stream`)
stream-stop:
    @docker exec spark-master pkill -f '[s]tream_table.py' && echo "stream stopped" || echo "stream not running"

# Append 10k new users + new versions of 40% of existing users to staging.core_user (append-only; stack must be up)
[working-directory: 'etl']
seed:
    uv run python src/data_gen/gen_staging_table.py
