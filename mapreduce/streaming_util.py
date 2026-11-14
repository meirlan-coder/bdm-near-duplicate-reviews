"""Small helpers shared by the Hadoop Streaming scripts.

Hadoop Streaming contract: records arrive on stdin one per line, the key is
the text before the first TAB (or the first N TAB-separated fields when
``stream.num.map.output.key.fields=N``), reducers receive their input sorted
by key, and a task can update job counters by writing
``reporter:counter:<group>,<name>,<amount>`` to stderr.
"""
import sys

from nddup import load_params

GROUP = "nddup"


class Counters(object):
    """Accumulate counters locally and flush them to stderr once at the end."""

    def __init__(self):
        self.values = {}

    def inc(self, name, amount=1):
        self.values[name] = self.values.get(name, 0) + amount

    def flush(self):
        for name, value in sorted(self.values.items()):
            sys.stderr.write("reporter:counter:%s,%s,%d\n" % (GROUP, name, value))
        sys.stderr.flush()
        self.values = {}


def params():
    return load_params()


def stdin_lines():
    # Hadoop Streaming feeds UTF-8 text; be robust to stray bytes.
    stream = open(sys.stdin.fileno(), "r", encoding="utf-8", errors="replace", newline="\n")
    for line in stream:
        yield line.rstrip("\n")


def stdout():
    return open(sys.stdout.fileno(), "w", encoding="utf-8", newline="\n", buffering=1 << 16)


def grouped(lines, num_key_fields=1):
    """Group consecutive lines with the same key: yields (key, [rest, ...])."""
    current, values = None, []
    for line in lines:
        parts = line.split("\t", num_key_fields)
        key = tuple(parts[:num_key_fields])
        rest = parts[num_key_fields] if len(parts) > num_key_fields else ""
        if key != current:
            if current is not None:
                yield current, values
            current, values = key, []
        values.append(rest)
    if current is not None:
        yield current, values
