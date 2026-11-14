# Benchmark summary

| dataset | framework | reviews | raw MB | prep s | detect s | total s | prep rev/s | detect rev/s | peak worker mem GB (prep / detect) | peak client mem GB (detect) | candidate pairs | similar pairs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1m | Hadoop MapReduce | 1,000,000 | 492 | 32.1 ± 1.1 | 195.8 ± 4.1 | 227.9 ± 4.7 | 31154 | 4271 | 2.93 / 3.59 | 0.34 | 13,456 | 2,640 |
| 1m | PySpark (cache on) | 1,000,000 | 492 | 32.3 ± 1.0 | 211.7 ± 2.3 | 244.0 ± 2.0 | 30986 | 3949 | 3.34 / 4.51 | 0.56 | 13,456 | 2,640 |
| 1m | PySpark (cache off) | 1,000,000 | 492 | 31.8 ± 0.5 | 257.3 ± 0.6 | 289.1 ± 0.3 | 31424 | 3250 | 3.39 / 4.36 | 0.56 | 13,456 | 2,640 |
| 5m | Hadoop MapReduce | 5,000,000 | 2460 | 109.2 ± 14.5 | 575.5 ± 32.9 | 684.7 ± 47.4 | 45788 | 7264 | 2.99 / 4.12 | 0.35 | 326,472 | 64,449 |
| 5m | PySpark (cache on) | 5,000,000 | 2460 | 96.2 ± 1.8 | 1101.8 ± 6.4 | 1198.0 ± 6.6 | 51983 | 3794 | 3.43 / 5.18 | 0.59 | 326,472 | 64,449 |

## MR vs Spark

* 1m: speed-up Spark/MR prep 0.99x, detect 0.92x, total 0.93x; identical output: True
* 5m: speed-up Spark/MR prep 1.14x, detect 0.52x, total 0.57x; identical output: True

## MapReduce job breakdown (mean seconds)

| dataset | P | D1 | D2 | D3 | D4 |
|---|---|---|---|---|---|
| 1m | 32.1 | 104.1 | 27.8 | 35.6 | 28.3 |
| 5m | 109.2 | 416.1 | 35.5 | 83.0 | 40.9 |
