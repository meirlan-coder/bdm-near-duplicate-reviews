#!/usr/bin/env bash
# Record the hardware and software environment of the experiment
# (results/env/env.json + raw command outputs in results/env/*.txt).
set -uo pipefail
. /etc/nddup-cluster.env 2>/dev/null || true
OUT=/opt/project/results/env
mkdir -p "$OUT"

lscpu > "$OUT/lscpu.txt" 2>&1
cat /proc/meminfo > "$OUT/meminfo.txt"
hadoop version > "$OUT/hadoop_version.txt" 2>&1
spark-submit --version > "$OUT/spark_version.txt" 2>&1
java -version > "$OUT/java_version.txt" 2>&1
yarn node -list -all -showDetails > "$OUT/yarn_nodes.txt" 2>&1
hdfs dfsadmin -report > "$OUT/hdfs_report.txt" 2>&1
df -h /localdata /opt/project > "$OUT/disk.txt" 2>&1

python3 - <<'EOF'
import json, os, platform, re, subprocess
import numpy
OUT = "/opt/project/results/env"
def txt(name):
    try:
        return open(os.path.join(OUT, name)).read()
    except OSError:
        return ""
lscpu = txt("lscpu.txt")
model = re.search(r"Model name:\s*(.+)", lscpu)
mem = re.search(r"MemTotal:\s*(\d+) kB", txt("meminfo.txt"))
hv = re.search(r"Hadoop (\S+)", txt("hadoop_version.txt"))
sv = re.search(r"version (\d+\.\d+\.\d+)", txt("spark_version.txt"))
jv = re.search(r'version "([^"]+)"', txt("java_version.txt"))
env = {
    "cpu_model": model.group(1).strip() if model else None,
    "logical_cpus_visible_to_docker": os.cpu_count(),
    "memory_visible_to_docker_gb": round(int(mem.group(1)) / 2**20, 2) if mem else None,
    "kernel": platform.release(),
    "hadoop": hv.group(1) if hv else None,
    "spark": sv.group(1) if sv else None,
    "java": jv.group(1) if jv else None,
    "python": platform.python_version(),
    "numpy": numpy.__version__,
    "cluster": {k: os.environ.get(k) for k in ("CONTAINER_MB", "CONTAINERS_PER_WORKER", "NM_MEMORY_MB",
                                                 "NM_VCORES", "TASK_SLOTS", "SPARK_EXECUTORS", "PARTITIONS")},
    "nodes": "1 master (NameNode, ResourceManager, JobHistory), 2 workers (DataNode + NodeManager), "
             "1 client (job submission / Spark driver); all Docker containers on one physical machine",
    "params": json.load(open("/opt/project/conf/lsh.json")),
}
host = os.path.join(OUT, "host.json")
if os.path.exists(host):
    try:
        env["host"] = json.load(open(host, encoding="utf-8-sig"))
    except ValueError:
        pass
json.dump(env, open(os.path.join(OUT, "env.json"), "w"), indent=2)
print(json.dumps(env, indent=2))
EOF
