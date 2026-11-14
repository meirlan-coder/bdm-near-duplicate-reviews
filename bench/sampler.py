#!/usr/bin/env python3
"""Container resource sampler (runs in the background in every container).

Every --interval seconds it appends one CSV line with the container-wide
memory and CPU usage read from the cgroup files of the container:

    ts, mem_bytes, anon_bytes, file_bytes, cpu_usec

* mem_bytes  : total memory charged to the container (incl. page cache)
* anon_bytes : anonymous memory = memory actually used by processes
               (JVMs + Python workers); this is the "memory usage" we report
* file_bytes : page cache (HDFS block files, shuffle files, ...)
* cpu_usec   : cumulative CPU time of all processes of the container

Works with cgroup v2 (Docker Desktop / WSL2, recent Linux) and cgroup v1.
The benchmark runner later cuts these time series by the start/end
timestamps of every job.
"""
import argparse
import os
import time

V2 = os.path.exists("/sys/fs/cgroup/cgroup.controllers")


def read(path, default=0):
    try:
        with open(path) as fh:
            return fh.read().strip()
    except OSError:
        return default


def mem_stat(path):
    out = {}
    for line in str(read(path, "")).splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1].isdigit():
            out[parts[0]] = int(parts[1])
    return out


def sample():
    if V2:
        mem = int(read("/sys/fs/cgroup/memory.current", 0) or 0)
        st = mem_stat("/sys/fs/cgroup/memory.stat")
        anon, file_ = st.get("anon", 0), st.get("file", 0)
        cpu = 0
        for line in str(read("/sys/fs/cgroup/cpu.stat", "")).splitlines():
            if line.startswith("usage_usec"):
                cpu = int(line.split()[1])
    else:
        mem = int(read("/sys/fs/cgroup/memory/memory.usage_in_bytes", 0) or 0)
        st = mem_stat("/sys/fs/cgroup/memory/memory.stat")
        anon, file_ = st.get("total_rss", st.get("rss", 0)), st.get("total_cache", st.get("cache", 0))
        cpu = int(read("/sys/fs/cgroup/cpuacct/cpuacct.usage", 0) or 0) // 1000
    return mem, anon, file_, cpu


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--interval", type=float, default=1.0)
    args = ap.parse_args()
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    new = not os.path.exists(args.out)
    with open(args.out, "a", buffering=1) as fh:
        if new:
            fh.write("ts,mem_bytes,anon_bytes,file_bytes,cpu_usec\n")
        while True:
            mem, anon, file_, cpu = sample()
            fh.write("%.3f,%d,%d,%d,%d\n" % (time.time(), mem, anon, file_, cpu))
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
