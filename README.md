# Near-Duplicate Detection of Product Reviews with MinHash + LSH: Hadoop MapReduce vs PySpark

Big Data Management, Master's Degree in Data Science, University of Messina (a.y. 2025/26).
Supervisor: Prof. Francesco La Rosa.

The project finds **near-duplicate reviews**, meaning pairs of reviews whose texts are almost identical, in millions of Amazon product reviews. Comparing every pair is not feasible: 5 million reviews give 1.25 × 10¹³ pairs. Instead, the pipeline uses **shingling, MinHash signatures and Locality-Sensitive Hashing (LSH)** (Leskovec, Rajaraman, Ullman, *Mining of Massive Datasets*, ch. 3). The same algorithm is implemented twice, with **Hadoop MapReduce** (Hadoop Streaming, Python) and with a **custom PySpark** implementation (RDD API). The two are benchmarked on the same YARN cluster, data, preprocessing, shingling, hash functions and Jaccard threshold.

> **Near-duplicate ≠ fake.** This project detects *textual similarity*. Many near-duplicates are legitimate: the same customer reviewing several variants of a product, a reviewer reusing their own text, short template phrases. Textual similarity alone does not show that a review is fraudulent. Fake-review detection would need further evidence, such as reviewer behaviour, timing, ratings and purchase verification, and is out of scope.

## Results at a glance

Measured on a Lenovo Legion Y540: Intel i5-9300HF (4 cores / 8 threads), 16 GB RAM, NVMe SSD, Docker Desktop on WSL2 with 7.7 GB. Each framework got 3 parallel 1 GB YARN containers. Figures are the mean of 3 runs (± standard deviation). Full tables: `results/summary/tables.md`.

| dataset | framework | preprocessing (s) | detection (s) | candidate pairs | similar pairs (J ≥ 0.8) |
|---|---|---|---|---|---|
| 1M reviews | Hadoop MapReduce | 32.1 ± 1.1 | **195.8 ± 4.1** | 13,456 | 2,640 |
| 1M reviews | PySpark (cache on) | 32.3 ± 1.0 | 211.7 ± 2.3 | 13,456 | 2,640 |
| 1M reviews | PySpark (cache off) | 31.8 ± 0.5 | 257.3 ± 0.6 | 13,456 | 2,640 |
| 5M reviews | Hadoop MapReduce | 109.2 ± 14.5 | **575.5 ± 32.9** | 326,472 | 64,449 |
| 5M reviews | PySpark (cache on) | 96.2 ± 1.8 | 1101.8 ± 6.4 | 326,472 | 64,449 |

* **Quality.** On a 33,429-review sample, exact all-pairs comparison (558.7 M pairs) gives 117 pairs with J ≥ 0.8. LSH found all of them (candidate recall 1.0) and verified only 146 pairs, 99.99997 % fewer comparisons.
* **Correctness.** MapReduce and Spark outputs are byte-identical in every run.
* **Speed.** The two frameworks are level at 1M reviews. At 5M, MapReduce is 1.9× faster. The Spark event logs show why: its `groupByKey`/`distinct` stages merge 84 M records inside Python workers and spill 3.7 GB, whereas Hadoop Streaming sorts and groups in the JVM (`results/summary/stage_breakdown.json`).
* **Spark wins** on small inputs (about 4× faster on the 33k sample, because each MapReduce job pays a fixed start-up cost). Caching cuts Spark's detection time by 18 % on 1M.

---

## 1. What is compared

| | Hadoop MapReduce | PySpark (custom) |
|---|---|---|
| Preprocessing (P) | 1 map-only job | 1 Spark application |
| Detection (D) | 4 chained jobs: D1 LSH buckets → D2 unique candidate pairs → D3 join texts → D4 exact Jaccard | 1 Spark application with the same 4 logical stages (`groupByKey` → `distinct` → 2 × `join` → filter) |
| Intermediate data | written to HDFS between jobs | kept in the DAG; the shingle-set RDD is **cached** (`MEMORY_AND_DISK`) because it is used 3 times (variant *spark-nocache* turns caching off) |
| Text logic | `src/nddup` (shared) | `src/nddup` (shared, the same module) |

**Same logic everywhere.** Parsing, normalisation, shingling, CRC32 shingle hashing, the MinHash hash family `h_i(x) = (a_i·x + b_i) mod p` (fixed seed), band hashing and the Jaccard computation live in one Python package, `src/nddup`. It is zipped (`build/nddup.zip`) and shipped to every map/reduce task (`-files`) and to every Spark executor (`--py-files`). All parameters come from one file, `conf/lsh.json`:

| parameter | value | meaning |
|---|---|---|
| `shingle_k` | 3 | word 3-shingles |
| `min_tokens` | 10 | reviews with fewer than 10 words are dropped in preprocessing (too short to be meaningful) |
| `num_bands` × `rows_per_band` | 20 × 5 | MinHash signature of 100 values; LSH threshold ≈ (1/20)^(1/5) ≈ 0.55 |
| `threshold` | **0.8** | a pair is reported if its exact Jaccard similarity ≥ 0.8 |
| `max_bucket` | 500 | LSH buckets with more than 500 reviews (large groups of identical template texts) are skipped and counted |
| `seed` | 42 | seed of the MinHash hash functions |

The probability that a pair with similarity *s* becomes a candidate is 1 − (1 − s⁵)²⁰. That is 0.9997 at s = 0.8 and 0.47 at s = 0.5.

**Same resources.** Both frameworks run on the same YARN cluster with the same container size (`CONTAINER_MB`, 1024 MB by default):

* Every MapReduce map or reduce task, every Spark executor (1 core) and both ApplicationMasters get one container.
* At most `2 × CONTAINERS_PER_WORKER − 1` tasks/executors run in parallel.
* Number of reducers = number of Spark shuffle partitions = 2 × parallel slots.
* Reducers start only after all maps finish (`slowstart = 1.0`), like Spark's stage barrier.
* Both frameworks keep their default shuffle settings.

## 2. Architecture

```
                       docker compose (one physical machine)
 ┌────────────┐   ┌──────────────────────────────────────────────────────────┐
 │  client    │   │ master: HDFS NameNode, YARN ResourceManager, JobHistory   │
 │ benchmark, │──▶├──────────────────────────────┬───────────────────────────┤
 │ MR client, │   │ worker1: DataNode+NodeManager│ worker2: DataNode+NodeMgr │
 │ Spark      │   │  map/reduce tasks or Spark   │  map/reduce tasks or Spark│
 │ driver     │   │  executors (YARN containers) │  executors                │
 └────────────┘   └──────────────────────────────┴───────────────────────────┘
   every container samples its own memory/CPU (cgroups) every second → results/metrics/
```

Software: Hadoop 3.3.6, Spark 3.5.3 (on YARN, client mode), Java 8, Python 3.10, NumPy. All containers use one image (`docker/Dockerfile`).

## 3. Data

[Amazon Reviews 2023](https://amazon-reviews-2023.github.io/) (McAuley Lab), category **Baby_Products** (≈ 6 million reviews, 2.95 GB of JSON lines). The input text of a review is `title + " " + text`.

| dataset | how it is built |
|---|---|
| `5m` | uniform random sample of 5,000,000 reviews (seed 2026) |
| `1m` | uniform random sample of 1,000,000 reviews of `5m` |
| `sample` | all reviews of randomly chosen users of `1m`, about 40,000 reviews. Used for the exact all-pairs evaluation. Sampling whole users keeps the pairs written by the same person, which is where most near-duplicates are, so the sample contains enough similar pairs |
| `smoke` | 20,000 synthetic reviews with planted near-duplicates (`scripts/make_synthetic.py`) |

The number of reviews and the size of every dataset are written to `results/data/manifest.json`. A larger optional dataset, `20m`, needs a bigger category: `CATEGORY=Beauty_and_Personal_Care SIZES=1m,5m,20m`.

## 4. Running the experiments

Requirements: Docker Desktop, about 30 GB of free disk space, and 8 GB or more of memory available to Docker.

**Windows (PowerShell, from the repository folder):**

```powershell
powershell -ExecutionPolicy Bypass -File .\run_all.ps1
Get-Content results\logs\run_all.log -Wait        # follow the progress
```

**Linux / macOS:** `./run_all.sh`, then `tail -f results/logs/run_all.log`.

The script builds the image, starts the cluster and runs `scripts/run_all.sh` inside the `client` container. That script:

1. waits for HDFS/YARN;
2. records the environment;
3. runs a smoke test that checks MapReduce output = Spark output = exact ground truth;
4. downloads the data and builds the datasets;
5. runs the evaluation against exact all-pairs;
6. runs the benchmarks;
7. writes tables and figures.

Each finished step is remembered in `results/.state/`, so if the run is interrupted, start the same command again and it continues where it stopped. With the default profile, the whole experiment takes several hours.

### Resource profiles (`.env`)

| memory available to Docker | `CONTAINERS_PER_WORKER` | parallel tasks / executors |
|---|---|---|
| ≈ 8 GB (16 GB laptop, WSL default) | 2 (default) | 3 |
| ≈ 12 GB | 3 | 5 |
| ≥ 16 GB | 4 | 7 |

On Windows, the memory Docker can use is set in `%UserProfile%\.wslconfig` (`[wsl2]` then `memory=12GB`). After changing `.env`, run `docker compose up -d --force-recreate`.

### Live demo

```bash
docker compose exec client bash scripts/demo.sh          # MR + Spark on the 40k sample, shows example pairs
```

Web UIs: YARN http://localhost:8088, HDFS http://localhost:9870, JobHistory http://localhost:19888, and the Spark UI at http://localhost:4040 while a Spark job runs.

### Running single steps

```bash
docker compose exec client bash
python3 bench/run_benchmark.py --datasets 1m --variants mr,spark --runs 3 --warmup 1
python3 bench/evaluate.py
python3 bench/analyze.py
```

## 5. Experimental protocol and metrics

* **Repetitions:** each configuration is run **3 times** after one discarded warm-up run on `1m` (the 1m runs also warm up the system for `5m`). Frameworks are interleaved (MR, Spark, Spark, MR, …) so that neither one always runs on a warmer system. Tables report mean ± standard deviation.
* **Execution time:** wall-clock time measured on the client, from job submission to completion. It includes YARN scheduling and JVM start-up, which are part of the real cost of each framework. **Preprocessing (P) and duplicate detection (D) are timed separately**, and every MR job is also timed on its own.
* **Throughput:** reviews/s and MB/s. Preprocessing uses input reviews; detection uses preprocessed reviews.
* **Memory usage:** anonymous (process) memory of each container, read from the cgroups every second. It excludes the page cache, which is reported separately. Per stage we report the peak of the sum over the two worker containers, where tasks and executors run, and the peak of the client container (Spark driver / MR job client). The per-task peak physical memory counters of MapReduce and the executor metrics in the Spark event logs (`results/spark-events`) are also stored.
* **Candidate pairs:** unique pairs that share at least one LSH bucket, plus raw pair emissions before de-duplication, oversized buckets, and the number of pairs verified and reported.
* **Caching:** the Spark variant `spark` persists the shingle-set RDD (`MEMORY_AND_DISK`). `spark-nocache` recomputes it, which shows how much caching contributes (measured on `1m`). MapReduce has no equivalent: each job reads its input from HDFS. The OS page cache is not dropped between runs, because containers cannot do that; interleaving and warm-up runs keep both frameworks in the same conditions.
* **Correctness:** the SHA-256 of the sorted output of every run is stored. The MR and Spark outputs must be identical.
* **Quality (sample):** exact Jaccard of **all** n(n−1)/2 pairs of the `sample` dataset, with the explicit threshold **J ≥ 0.8**. Reported: number of true similar pairs, **candidate recall** |candidates ∩ truth| / |truth|, final recall and precision, **reduction of pairwise comparisons** 1 − |candidates| / (n(n−1)/2), the empirical vs theoretical S-curve, and a (b, r) sensitivity study.

## 6. Limitations

* **Single machine.** All containers share one laptop's CPUs, memory and disk, so this is a *pseudo-distributed* cluster. Results show how the frameworks behave under equal, constrained resources. They do **not** measure network effects or true horizontal scalability; scaling is studied only in data size (1M → 5M).
* HDFS replication is 1 (one disk), so data locality and fault tolerance are not representative of a real cluster.
* YARN memory checks are disabled. Container sizes are *scheduling* quotas, and the actual usage is measured with cgroups.
* Buckets with more than `max_bucket` reviews are skipped (and counted). Pairs found only in such buckets are lost.
* Near-duplicate ≠ fake review (see above).

## 7. Repository layout

```
conf/lsh.json             shared parameters (k, b, r, seed, threshold, ...)
src/nddup/                shared text / MinHash / LSH logic (used by both frameworks)
mapreduce/                Hadoop Streaming mappers & reducers (+ local_runner.py for tests)
spark/                    PySpark jobs: prep_job.py, detect_job.py
bench/                    pipelines.py (job submission), run_benchmark.py, evaluate.py,
                          exact_allpairs.py, analyze.py, sampler.py, smoke_test.py, show_pairs.py
scripts/                  prepare_data.py, run_all.sh, collect_env.sh, demo.sh, make_synthetic.py
docker/                   Dockerfile, entrypoint.sh, Hadoop/Spark configuration templates
docker-compose.yml, .env  cluster definition and resource profile
tests/                    unit tests (pytest)
results/                  everything produced by the experiments (raw runs, metrics, tables, figures)
```

Development without Docker: `pip install numpy scipy pyspark==3.5.3 pytest matplotlib`, then `pytest tests`. With `NDDUP_LOCAL_DIR=/some/dir`, the benchmark scripts run in a local emulation mode (MapReduce through `mapreduce/local_runner.py`, Spark `local[2]`). That mode is only for checking correctness; it is never used for the reported numbers.
