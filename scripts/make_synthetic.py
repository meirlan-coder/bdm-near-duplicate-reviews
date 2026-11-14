#!/usr/bin/env python3
"""Generate a synthetic review file in the Amazon Reviews 2023 JSONL format.

Used only for the smoke test and the unit/integration tests, so that the whole
pipeline can be checked in a few seconds without downloading the real data.
Near-duplicates are planted on purpose: a fraction of the reviews are edited
copies (word substitutions / deletions / insertions) of earlier reviews.

    python3 scripts/make_synthetic.py --n 20000 --out data/synthetic.jsonl
"""
import argparse
import json
import random


def build_vocab(rng, size):
    letters = "abcdefghijklmnopqrstuvwxyz"
    vocab = set()
    while len(vocab) < size:
        vocab.add("".join(rng.choice(letters) for _ in range(rng.randint(2, 9))))
    return sorted(vocab)


def mutate(rng, words, rate, vocab):
    out = []
    for w in words:
        u = rng.random()
        if u < rate / 3:
            continue                                # deletion
        if u < 2 * rate / 3:
            out.append(rng.choice(vocab))           # substitution
            continue
        out.append(w)
        if u < rate:
            out.append(rng.choice(vocab))           # insertion
    return out or words


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=20000)
    ap.add_argument("--dup-fraction", type=float, default=0.08)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    vocab = build_vocab(rng, 3000)
    weights = [1.0 / (i + 1) for i in range(len(vocab))]      # Zipf-like
    users = ["U%06d" % i for i in range(max(10, args.n // 4))]
    written = []
    with open(args.out, "w", encoding="utf-8") as fh:
        for i in range(args.n):
            if written and rng.random() < args.dup_fraction:
                base_words, base_user = rng.choice(written)
                rate = rng.choice([0.0, 0.02, 0.05, 0.1, 0.2, 0.3, 0.5])
                words = mutate(rng, base_words, rate, vocab)
                user = base_user if rng.random() < 0.7 else rng.choice(users)
            else:
                length = max(3, int(rng.lognormvariate(3.4, 0.7)))
                words = rng.choices(vocab, weights=weights, k=length)
                user = rng.choice(users)
            written.append((words, user))
            rec = {
                "rating": float(rng.randint(1, 5)),
                "title": " ".join(words[:3]).title(),
                "text": " ".join(words[3:]) + ("<br />" if rng.random() < 0.1 else ""),
                "images": [],
                "asin": "B%09d" % rng.randint(0, 10 ** 6),
                "parent_asin": "B%09d" % rng.randint(0, 10 ** 6),
                "user_id": user,
                "timestamp": 1500000000000 + i,
                "helpful_vote": 0,
                "verified_purchase": True,
                "review_id": "R%09d" % i,
            }
            fh.write(json.dumps(rec) + "\n")


if __name__ == "__main__":
    main()
