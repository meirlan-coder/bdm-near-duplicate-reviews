#!/usr/bin/env bash
# Start the daemons of one cluster node.  Usage: entrypoint.sh master|worker|client
set -euo pipefail
ROLE="${1:-client}"

# ---- resource profile (from .env) ------------------------------------------
C="${CONTAINER_MB:-1024}"              # size of every YARN container
CPW="${CONTAINERS_PER_WORKER:-3}"      # containers per NodeManager
WORKERS=2
export NM_MEMORY_MB=$(( C * CPW ))
export NM_VCORES=${CPW}
export AM_MB=${C}
export TASK_HEAP_MB=$(( C * 5 / 8 ))                  # MR task JVM heap (-Xmx)
export SPARK_EXECUTOR_HEAP_MB=$(( C / 2 ))            # spark.executor.memory
export SPARK_EXECUTOR_OVERHEAD_MB=$(( C / 2 ))        # off-heap + Python worker
export SPARK_AM_HEAP_MB=$(( C - 384 ))
export TASK_SLOTS=$(( WORKERS * CPW - 1 ))            # one container is the AM
export SPARK_EXECUTORS=${TASK_SLOTS}
export PARTITIONS=${PARTITIONS:-$(( 2 * TASK_SLOTS ))}

cat > /etc/nddup-cluster.env <<EOF
export CONTAINER_MB=${C}
export CONTAINERS_PER_WORKER=${CPW}
export NM_MEMORY_MB=${NM_MEMORY_MB}
export NM_VCORES=${NM_VCORES}
export TASK_SLOTS=${TASK_SLOTS}
export SPARK_EXECUTORS=${SPARK_EXECUTORS}
export PARTITIONS=${PARTITIONS}
EOF
grep -q nddup-cluster.env /root/.bashrc 2>/dev/null || echo ". /etc/nddup-cluster.env" >> /root/.bashrc

# ---- render configuration templates -----------------------------------------
VARS='${NM_MEMORY_MB} ${NM_VCORES} ${AM_MB} ${TASK_HEAP_MB} ${CONTAINER_MB_} ${SPARK_EXECUTOR_HEAP_MB} ${SPARK_EXECUTOR_OVERHEAD_MB} ${SPARK_AM_HEAP_MB} ${SPARK_EXECUTORS}'
export CONTAINER_MB_=${C}
for f in /opt/project/docker/conf/hadoop/*.xml; do
  envsubst "${VARS}" < "$f" > "${HADOOP_CONF_DIR}/$(basename "$f")"
done
envsubst "${VARS}" < /opt/project/docker/conf/spark/spark-defaults.conf > "${SPARK_HOME}/conf/spark-defaults.conf"
grep -q "^export JAVA_HOME=${JAVA_HOME}" "${HADOOP_CONF_DIR}/hadoop-env.sh" || \
  echo "export JAVA_HOME=${JAVA_HOME}" >> "${HADOOP_CONF_DIR}/hadoop-env.sh"

mkdir -p /opt/project/results/metrics /opt/project/results/logs /opt/project/results/spark-events /data

# ---- resource sampler (memory / CPU of this container, every second) --------
nohup python3 /opt/project/bench/sampler.py --out "/opt/project/results/metrics/$(hostname).csv" \
  > /dev/null 2>&1 &

case "${ROLE}" in
  master)
    if [ ! -f /data/nn/current/VERSION ]; then
      hdfs namenode -format -force -nonInteractive
    fi
    hdfs --daemon start namenode
    yarn --daemon start resourcemanager
    (
      hdfs dfsadmin -safemode wait
      hdfs dfs -mkdir -p /tmp /user/root /nddup /tmp/logs /mr-history
      hdfs dfs -chmod -R 1777 /tmp
      mapred --daemon start historyserver
      echo "master ready"
    ) > /opt/hadoop/logs/master-init.log 2>&1 &
    ;;
  worker)
    hdfs --daemon start datanode
    yarn --daemon start nodemanager
    ;;
  client)
    ;;
  *)
    echo "unknown role ${ROLE}"; exit 1 ;;
esac

echo "[entrypoint] ${ROLE} started (NodeManager: ${NM_MEMORY_MB} MB / ${NM_VCORES} vcores; container ${C} MB)"
exec sleep infinity
