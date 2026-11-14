"""Submit the MapReduce and Spark pipelines to the YARN cluster.

Used by run_benchmark.py, evaluate.py and the smoke test.  Every function
returns a list of *job records* (dicts) containing wall-clock start/end times
measured on the client and the counters reported by the framework.
"""
import glob
import json
import os
import re
import subprocess
import sys
import time

PROJECT = os.environ.get("PROJECT_DIR", "/opt/project")
HADOOP_HOME = os.environ.get("HADOOP_HOME", "/opt/hadoop")
SPARK_HOME = os.environ.get("SPARK_HOME", "/opt/spark")
ZIP = os.path.join(PROJECT, "build", "nddup.zip")
PARAMS_FILE = os.path.join(PROJECT, "conf", "lsh.json")
HDFS_ROOT = "/nddup"

# Development/CI mode: when NDDUP_LOCAL_DIR is set, "HDFS" paths are mapped to
# that local directory, MapReduce jobs run through mapreduce/local_runner.py
# and Spark runs with master local[2].  Never used for the reported results.
LOCAL_DIR = os.environ.get("NDDUP_LOCAL_DIR")


def L(path):
    return os.path.join(LOCAL_DIR, path.lstrip("/"))


def local_parts(path):
    p = L(path)
    if os.path.isdir(p):
        files = sorted(glob.glob(os.path.join(p, "part-*"))) or sorted(
            f for f in glob.glob(os.path.join(p, "*")) if os.path.isfile(f))
        return files
    return sorted(glob.glob(p))


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------
def sh(cmd, log=None, check=True):
    """Run a command, tee stdout+stderr to `log`, return (rc, output)."""
    proc = subprocess.run(cmd, shell=isinstance(cmd, str), stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, universal_newlines=True)
    if log:
        with open(log, "a") as fh:
            fh.write("$ %s\n%s\n" % (cmd if isinstance(cmd, str) else " ".join(cmd), proc.stdout))
    if check and proc.returncode != 0:
        tail = "\n".join(proc.stdout.splitlines()[-40:])
        raise RuntimeError("command failed (%d): %s\n%s" % (proc.returncode, cmd, tail))
    return proc.returncode, proc.stdout


def build_zip():
    """Package src/nddup as build/nddup.zip (shipped to every task / executor)."""
    import zipfile
    os.makedirs(os.path.dirname(ZIP), exist_ok=True)
    with zipfile.ZipFile(ZIP, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(glob.glob(os.path.join(PROJECT, "src", "nddup", "*.py"))):
            zf.write(path, os.path.join("nddup", os.path.basename(path)))
    return ZIP


def load_params_env():
    from_file = json.load(open(PARAMS_FILE))
    return {"NDDUP_" + k.upper(): str(v) for k, v in from_file.items()}


def hdfs_rm(path, log=None):
    if LOCAL_DIR:
        import shutil
        shutil.rmtree(L(path), ignore_errors=True)
        return
    sh(["hdfs", "dfs", "-rm", "-r", "-f", "-skipTrash", path], log, check=False)


def hdfs_du(path):
    if LOCAL_DIR:
        return sum(os.path.getsize(f) for f in local_parts(path)) if os.path.exists(L(path)) else None
    rc, out = sh(["hdfs", "dfs", "-du", "-s", path], check=False)
    try:
        return int(out.strip().splitlines()[-1].split()[0]) if rc == 0 else None
    except (ValueError, IndexError):
        return None


def hdfs_lines(path):
    """Return the sorted lines of all part files below an HDFS directory."""
    if LOCAL_DIR:
        return sorted(l.rstrip("\n") for f in local_parts(path) for l in open(f, encoding="utf-8") if l.strip())
    rc, out = sh("hdfs dfs -cat '%s/part-*' 2>/dev/null" % path, check=False)
    return sorted(l for l in out.splitlines() if l.strip()) if rc == 0 else []


def hdfs_getmerge(path, local_file):
    if LOCAL_DIR:
        with open(local_file, "w", encoding="utf-8") as out:
            for f in local_parts(path):
                out.write(open(f, encoding="utf-8").read())
        return
    sh(["hdfs", "dfs", "-getmerge", path, local_file])


def slots():
    return int(os.environ.get("TASK_SLOTS", "3"))


def default_partitions():
    return int(os.environ.get("PARTITIONS", str(2 * slots())))


# --------------------------------------------------------------------------
# Hadoop MapReduce (Hadoop Streaming)
# --------------------------------------------------------------------------
_COUNTER_RE = re.compile(r"^\s+([^=\t][^=]*)=(\d+)\s*$")
_JOBID_RE = re.compile(r"Running job: (job_\d+_\d+)")


def parse_mr_counters(output):
    counters = {}
    for line in output.splitlines():
        m = _COUNTER_RE.match(line)
        if m:
            counters[m.group(1).strip()] = int(m.group(2))
    jid = _JOBID_RE.search(output)
    return counters, (jid.group(1) if jid else None)


def streaming_jar():
    jars = glob.glob(os.path.join(HADOOP_HOME, "share/hadoop/tools/lib/hadoop-streaming-*.jar"))
    return jars[0] if jars else "hadoop-streaming.jar"


def mr_job(name, inputs, output, mapper, reducer=None, reduces=None, key_fields=1, log=None):
    mr_dir = os.path.join(PROJECT, "mapreduce")
    files = [os.path.join(mr_dir, "streaming_util.py"), ZIP]
    for script in (mapper, reducer):
        if script and script.endswith(".py"):
            files.append(os.path.join(mr_dir, script))
    cmd = ["hadoop", "jar", streaming_jar(),
           "-D", "mapreduce.job.name=%s" % name,
           "-D", "mapreduce.job.reduces=%d" % (0 if reducer is None else reduces),
           "-D", "stream.num.map.output.key.fields=%d" % key_fields,
           "-files", ",".join(files)]
    cmd += ["-cmdenv", "PYTHONPATH=.:nddup.zip", "-cmdenv", "PYTHONIOENCODING=utf-8"]
    for k, v in sorted(load_params_env().items()):
        cmd += ["-cmdenv", "%s=%s" % (k, v)]
    for inp in inputs:
        cmd += ["-input", inp]
    cmd += ["-output", output,
            "-mapper", mapper if mapper == "cat" else "python3 %s" % mapper]
    if reducer is not None:
        cmd += ["-reducer", reducer if reducer == "cat" else "python3 %s" % reducer]
    hdfs_rm(output, log)
    start = time.time()
    if LOCAL_DIR:
        sys.path.insert(0, mr_dir)
        import local_runner
        os.environ.update(load_params_env())
        files_in = [f for i in inputs for f in local_parts(i)]
        counters = local_runner.run_job(name, files_in, L(output), mapper, reducer,
                                        reduces or 1, key_fields)
        job_id = "local"
        end = time.time()
    else:
        _, out = sh(cmd, log)
        end = time.time()
        counters, job_id = parse_mr_counters(out)
    return {"job": name, "start": start, "end": end,
            "seconds": end - start, "job_id": job_id, "counters": counters}


def mr_prep(raw, work, tag, log=None):
    rec = mr_job("nddup-mr-P-%s" % tag, [raw], work + "/prep", "prep_mapper.py", log=log)
    rec["stage_job"] = "P"
    return [rec]


def mr_detect(work, tag, reduces=None, log=None):
    R = reduces or default_partitions()
    jobs = [
        ("D1", [work + "/prep"], work + "/buckets", "lsh_mapper.py", "bucket_reducer.py", 1),
        ("D2", [work + "/buckets"], work + "/pairs", "cat", "dedup_reducer.py", 2),
        ("D3", [work + "/prep", work + "/pairs"], work + "/joined", "join_mapper.py", "join_reducer.py", 1),
        ("D4", [work + "/joined"], work + "/similar", "cat", "verify_reducer.py", 2),
    ]
    recs = []
    for jid, inputs, output, mapper, reducer, kf in jobs:
        rec = mr_job("nddup-mr-%s-%s" % (jid, tag), inputs, output, mapper, reducer, R, kf, log)
        rec["stage_job"] = jid
        recs.append(rec)
    return recs


# --------------------------------------------------------------------------
# Spark (custom RDD implementation, on YARN)
# --------------------------------------------------------------------------
def spark_submit(script, args, name, log=None):
    tmp = os.path.join(PROJECT, "results", "tmp")
    os.makedirs(tmp, exist_ok=True)
    metrics_file = os.path.join(tmp, "%s.json" % name)
    if os.path.exists(metrics_file):
        os.remove(metrics_file)
    submit = os.environ.get("SPARK_SUBMIT", os.path.join(SPARK_HOME, "bin", "spark-submit"))
    if LOCAL_DIR:
        args = [L(a) if a.startswith(HDFS_ROOT) else a for a in args]
        submit = [submit, "--master", "local[2]"]
    else:
        submit = [submit]
    cmd = submit + ["--name", name,
           "--py-files", ZIP, os.path.join(PROJECT, "spark", script),
           "--params", PARAMS_FILE, "--metrics", metrics_file] + args
    start = time.time()
    _, out = sh(cmd, log)
    end = time.time()
    counters = json.load(open(metrics_file)) if os.path.exists(metrics_file) else {}
    return {"job": name, "start": start, "end": end, "seconds": end - start,
            "job_id": counters.get("application_id"), "counters": counters}


def spark_prep(raw, work, tag, log=None):
    hdfs_rm(work + "/prep", log)
    rec = spark_submit("prep_job.py", ["--input", raw, "--output", work + "/prep"],
                       "nddup-spark-P-%s" % tag, log)
    rec["stage_job"] = "P"
    return [rec]


def spark_detect(work, tag, partitions=None, cache="on", save_candidates=False, log=None):
    R = partitions or default_partitions()
    hdfs_rm(work + "/similar", log)
    args = ["--input", work + "/prep", "--output", work + "/similar",
            "--partitions", str(R), "--cache", cache]
    if save_candidates:
        hdfs_rm(work + "/pairs", log)
        args += ["--save-candidates", work + "/pairs"]
    rec = spark_submit("detect_job.py", args, "nddup-spark-D-%s" % tag, log)
    rec["stage_job"] = "D"
    return [rec]


# --------------------------------------------------------------------------
def run_pipeline(framework, raw, work, tag, cache="on", save_candidates=False, log=None):
    """Run preprocessing + detection, return {'prep': [...], 'detect': [...]}."""
    hdfs_rm(work, log)
    if framework == "mr":
        prep = mr_prep(raw, work, tag, log)
        detect = mr_detect(work, tag, log=log)
    elif framework == "spark":
        prep = spark_prep(raw, work, tag, log)
        detect = spark_detect(work, tag, cache=cache, save_candidates=save_candidates, log=log)
    else:
        raise ValueError(framework)
    return {"prep": prep, "detect": detect}


def summarize_counters(framework, stages):
    """Normalise the counters of both frameworks to the same names."""
    merged = {}
    for rec in stages["prep"] + stages["detect"]:
        for k, v in rec["counters"].items():
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                merged[k] = merged.get(k, 0) + v if framework == "mr" else v
    keys = ["prep_input_reviews", "prep_output_reviews", "prep_dropped_reviews",
            "lsh_docs_signed", "lsh_nonsingleton_buckets", "lsh_pair_emissions",
            "lsh_oversized_buckets", "lsh_reviews_in_oversized_buckets",
            "candidate_pairs", "verified_pairs", "similar_pairs"]
    return {k: merged.get(k, 0) for k in keys}
