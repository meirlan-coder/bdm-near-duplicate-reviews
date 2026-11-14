#!/usr/bin/env python3
"""Spark job P: raw JSON reviews -> 'review_id<TAB>normalised text' on HDFS.

Same logic as the MapReduce prep job (both call nddup.core.preprocess_line).

    spark-submit --py-files build/nddup.zip spark/prep_job.py \
        --input hdfs:///nddup/raw/1m --output hdfs:///nddup/work/spark/1m/prep \
        --params conf/lsh.json --metrics results/tmp/metrics.json
"""
import argparse
import json
import time

from pyspark import SparkConf, SparkContext

from nddup import Params, load_params
from nddup.core import preprocess_line


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--params", default=None)
    ap.add_argument("--metrics", default=None, help="local JSON file for counters")
    args = ap.parse_args()

    p = load_params(args.params)
    sc = SparkContext(conf=SparkConf().setAppName("nddup-prep"))
    sc.setLogLevel("WARN")
    t0 = time.time()

    n_in = sc.accumulator(0)
    n_out = sc.accumulator(0)
    params_json = p.to_json()

    def prep_partition(lines):
        local = Params(**json.loads(params_json))
        cin = cout = 0
        for line in lines:
            cin += 1
            rec = preprocess_line(line, local)
            if rec is not None:
                cout += 1
                yield rec
        n_in.add(cin)
        n_out.add(cout)

    sc.textFile(args.input).mapPartitions(prep_partition).saveAsTextFile(args.output)

    metrics = {
        "prep_input_reviews": n_in.value,
        "prep_output_reviews": n_out.value,
        "prep_dropped_reviews": n_in.value - n_out.value,
        "driver_seconds": time.time() - t0,
        "application_id": sc.applicationId,
    }
    print("NDDUP_METRICS " + json.dumps(metrics))
    if args.metrics:
        with open(args.metrics, "w") as fh:
            json.dump(metrics, fh)
    sc.stop()


if __name__ == "__main__":
    main()
