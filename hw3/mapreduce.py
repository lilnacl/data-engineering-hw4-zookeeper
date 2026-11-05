import argparse
import csv
import io
import itertools
import json
import os
import time
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from pathlib import Path

from .common import BUCKET, KEY, dump, is_variant, normalize_skills, s3_client


def register_worker():
    registry = os.getenv('HW_PID_REGISTRY')
    if registry:
        fd = os.open(registry, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
        try:
            os.write(fd, f'{os.getpid()}\n'.encode())
        finally:
            os.close(fd)


class S3Raw(io.RawIOBase):
    def __init__(self, body):
        self.body = body
        self.bytes_read = 0

    def readable(self):
        return True

    def readinto(self, buffer):
        data = self.body.read(len(buffer))
        buffer[:len(data)] = data
        self.bytes_read += len(data)
        return len(data)


def csv_chunks(raw, chunk_rows: int, prefilter=False):
    csv.field_size_limit(16 * 1024 * 1024)
    text = io.TextIOWrapper(io.BufferedReader(raw, buffer_size=1024 * 1024), encoding='utf-8-sig', newline='')
    rows = csv.DictReader(text)
    raw.csv_rows = 0
    def projected():
        for row in rows:
            raw.csv_rows += 1
            if not prefilter or is_variant(row):
                yield {k: row[k] for k in ('name', 'role', 'key_skills')}
    compact = projected()
    while chunk := list(itertools.islice(compact, chunk_rows)):
        yield chunk


def map_chunk(rows, combine: bool, fail: bool = False):
    if fail:
        raise RuntimeError('Injected mapper failure before committing chunk output')
    outputs = [Counter(), Counter(), Counter()] if combine else [[], [], []]
    selected = 0
    total_emitted = [0, 0, 0]
    for row in rows:
        if not is_variant(row):
            continue
        selected += 1
        skills = normalize_skills(row['key_skills'])
        for i in range(3):
            combinations = list(itertools.combinations(skills, i + 1))
            total_emitted[i] += len(combinations)
            if combine:
                outputs[i].update(combinations)
            else:
                outputs[i].extend(combinations)
    return outputs, selected, total_emitted


def reduce_chunk(counts, result, combine):
    for destination, values in zip(counts, result[0]):
        destination.update(values)


def run(chunk_rows=2000, workers=4, combine=True, inject_failure=False, prefilter=False):
    started = time.perf_counter()
    body = s3_client().get_object(Bucket=BUCKET, Key=KEY)['Body']
    raw = S3Raw(body)
    counts = [Counter(), Counter(), Counter()]
    selected = seen = chunks = retries = 0
    emitted = [0, 0, 0]
    transferred = [0, 0, 0]
    try:
        with ProcessPoolExecutor(max_workers=workers, initializer=register_worker) as pool:
            pending = {}
            iterator = iter(csv_chunks(raw, chunk_rows, prefilter))
            exhausted = False
            while pending or not exhausted:
                while len(pending) < workers * 2 and not exhausted:
                    try:
                        chunk = next(iterator)
                    except StopIteration:
                        exhausted = True
                        break
                    number = chunks
                    chunks += 1
                    seen += len(chunk)
                    future = pool.submit(map_chunk, chunk, combine, inject_failure and number == 1)
                    pending[future] = (number, chunk, 0)
                if not pending:
                    continue
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    number, chunk, attempt = pending.pop(future)
                    try:
                        result = future.result()
                    except RuntimeError:
                        if attempt >= 1:
                            raise
                        retries += 1
                        print(f'chunk={number} attempt=1 failed; retry=2', flush=True)
                        pending[pool.submit(map_chunk, chunk, combine, False)] = (number, chunk, attempt + 1)
                        continue
                    reduce_chunk(counts, result, combine)
                    selected += result[1]
                    for i in range(3):
                        emitted[i] += result[2][i]
                        transferred[i] += len(result[0][i])
    finally:
        body.close()
    stats = {'rows': raw.csv_rows, 'selected_rows': selected, 'chunks': chunks, 'chunk_rows': chunk_rows,
             'workers': workers, 'combiner': combine, 'retries': retries, 's3_bytes': raw.bytes_read,
             'prefilter': prefilter, 'dispatched_rows': seen,
             'emitted': emitted, 'transferred_keys': transferred, 'seconds': time.perf_counter() - started}
    return counts, stats


def write_counts(destination, counts):
    for k, counter in enumerate(counts, 1):
        dump(destination / f'counts_{k}.json', [{'skills': list(key), 'count': count} for key, count in sorted(counter.items())])


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--chunk-rows', type=int, default=2000)
    p.add_argument('--workers', type=int, default=4)
    p.add_argument('--no-combiner', action='store_true')
    p.add_argument('--inject-failure', action='store_true')
    p.add_argument('--prefilter', action='store_true')
    a = p.parse_args()
    counters, stats = run(a.chunk_rows, a.workers, not a.no_combiner, a.inject_failure, a.prefilter)
    write_counts(a.output, counters)
    dump(a.output / 'stats.json', stats)
    print(json.dumps(stats, indent=2), flush=True)
