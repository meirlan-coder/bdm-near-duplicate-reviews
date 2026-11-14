#!/usr/bin/env bash
# Complete experiment, executed inside the client container:
#   docker compose exec client bash scripts/run_all.sh
#
# Steps (each one is skipped if it already finished, so the script can simply
# be started again after an interruption):
#   1. wait for HDFS and YARN        5. evaluation vs exact all-pairs (sample)
#   2. record the environment        6. benchmark 1m  (MR, Spark, Spark without cache)
#   3. smoke test (synthetic data)   7. benchmark 5m  (MR, Spark)
#   4. download + prepare datasets   8. tables and figures
#
# Environment overrides: CATEGORY (default Baby_Products), SIZES (1m,5m),
# RUNS (3), STEPS (all).
set -euo pipefail
cd /opt/project
until [ -f /etc/nddup-cluster.env ]; do sleep 1; done
. /etc/nddup-cluster.env
CATEGORY="${CATEGORY:-Baby_Products}"
SIZES="${SIZES:-1m,5m}"
RUNS="${RUNS:-3}"
STATE=results/.state
mkdir -p "$STATE" results/logs

step() {  # step <name> <command...>
  local name=$1; shift
  if [ -f "$STATE/$name.done" ]; then
    echo "=== [$(date +%H:%M:%S)] $name: already done, skipping"
    return 0
  fi
  echo "=== [$(date +%H:%M:%S)] $name: starting"
  "$@"
  touch "$STATE/$name.done"
  echo "=== [$(date +%H:%M:%S)] $name: finished"
}

wait_cluster() {
  echo "waiting for HDFS ..."
  until hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; do sleep 5; done
  echo "waiting for 2 YARN NodeManagers ..."
  until [ "$(yarn node -list 2>/dev/null | grep -c RUNNING)" -ge 2 ]; do sleep 5; done
  until nc -z master 10020; do echo "waiting for JobHistory server ..."; sleep 5; done
  hdfs dfs -mkdir -p /nddup
  echo "cluster ready: ${TASK_SLOTS} parallel task slots, ${PARTITIONS} reducers/partitions"
}

wait_cluster
step env       bash scripts/collect_env.sh
step smokedata python3 scripts/prepare_data.py --smoke-only
step smoke     python3 bench/smoke_test.py
step data      python3 scripts/prepare_data.py --category "$CATEGORY" --sizes "$SIZES"
step eval      python3 bench/evaluate.py
step bench_1m  python3 bench/run_benchmark.py --datasets 1m --variants mr,spark,spark-nocache --runs "$RUNS" --warmup 1
for ds in ${SIZES//,/ }; do
  [ "$ds" = "1m" ] && continue
  step "bench_$ds" python3 bench/run_benchmark.py --datasets "$ds" --variants mr,spark --runs "$RUNS" --warmup 0
done
python3 bench/analyze.py
echo "=== ALL DONE: results are in the results/ folder (summary in results/summary/tables.md)"
