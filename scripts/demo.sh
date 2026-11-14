#!/usr/bin/env bash
# Live demo for the exam (run inside the client container):
#   docker compose exec client bash scripts/demo.sh [dataset]
# Runs the MapReduce and the Spark pipeline on a small dataset (default: the
# 40k-review evaluation sample), checks that both outputs are identical and
# shows some detected near-duplicate reviews.
set -euo pipefail
cd /opt/project
. /etc/nddup-cluster.env
DS="${1:-sample}"
python3 - "$DS" <<'EOF'
import sys, time
sys.path.insert(0, "bench")
import pipelines as pl
ds = sys.argv[1]
pl.build_zip()
res = {}
for fw in ("mr", "spark"):
    work = "/nddup/work/demo-%s" % fw
    print("\n>>> %s on dataset '%s' ..." % ("Hadoop MapReduce (5 jobs)" if fw == "mr" else "PySpark (2 applications)", ds), flush=True)
    st = pl.run_pipeline(fw, "/nddup/raw/" + ds, work, "demo-" + ds)
    c = pl.summarize_counters(fw, st)
    res[fw] = pl.hdfs_lines(work + "/similar")
    for j in st["prep"] + st["detect"]:
        print("    job %-3s %6.1f s" % (j["stage_job"], j["seconds"]))
    print("    reviews %d -> after preprocessing %d | candidate pairs %d | similar pairs (J >= threshold) %d"
          % (c["prep_input_reviews"], c["prep_output_reviews"], c["candidate_pairs"], len(res[fw])))
print("\nIdentical results MapReduce vs Spark: %s\n" % (res["mr"] == res["spark"]))
EOF
python3 bench/show_pairs.py --work /nddup/work/demo-spark --n 6
