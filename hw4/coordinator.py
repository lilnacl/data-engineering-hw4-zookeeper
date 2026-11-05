"""A resumable HW3 coordinator with deliberately optional result fencing."""

import argparse
import hashlib
import json
import os
import threading
import time
from collections import deque
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError, NodeExistsError, NoNodeError
from kazoo.retry import KazooRetry

from hw3.mapreduce import S3Raw, csv_chunks, map_chunk
from hw4.checkpoint import State, atomic_json, load_checkpoint, pair_records, save_checkpoint
from hw4.coordination import EpochStore


class Journal:
    def __init__(self, path: Path, coordinator: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.stream = path.open("a")
        self.coordinator = coordinator
        self.lock = threading.Lock()

    def event(self, event: str, **fields) -> None:
        record = {"time": datetime.now(timezone.utc).isoformat(),
                  "monotonic_ns": time.monotonic_ns(), "pid": os.getpid(),
                  "coordinator": self.coordinator, "event": event, **fields}
        with self.lock:
            self.stream.write(json.dumps(record, ensure_ascii=False) + "\n")
            self.stream.flush()


def source_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def become_candidate(zk, store: EpochStore, journal: Journal, wake: threading.Event) -> str | None:
    candidate = None
    while True:
        children = sorted(zk.get_children(store.root + "/election"))
        if candidate is None or candidate.rsplit("/", 1)[-1] not in children:
            candidate = zk.create(store.root + "/election/candidate-",
                                  journal.coordinator.encode(), ephemeral=True, sequence=True)
            journal.event("candidate", path=candidate)
            children = sorted(zk.get_children(store.root + "/election"))
        if store.result()[0] is not None:
            journal.event("already_completed")
            return None
        own = candidate.rsplit("/", 1)[-1]
        if own not in children:
            candidate = None
            continue
        position = children.index(own)
        if position == 0:
            return candidate
        predecessor = store.root + "/election/" + children[position - 1]
        wake.clear()
        if zk.exists(predecessor, watch=lambda _: wake.set()):
            wake.wait(timeout=30)


def calculate(args, zk, store: EpochStore, token, state: State, journal: Journal) -> dict:
    pending = deque()
    checkpoint_allowed = True
    reconnect = KazooRetry(max_tries=-1, delay=0.1, max_delay=1, deadline=20, ignore_expire=True)
    with args.source.open("rb") as body:
        raw = S3Raw(body)
        iterator = iter(enumerate(csv_chunks(raw, args.chunk_rows)))
        exhausted = False
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            while pending or not exhausted:
                while len(pending) < args.workers * 2 and not exhausted:
                    try:
                        number, chunk = next(iterator)
                    except StopIteration:
                        exhausted = True
                        break
                    if number < state.next_chunk:
                        continue
                    pending.append((number, chunk, pool.submit(map_chunk, chunk, True)))
                if not pending:
                    continue
                number, chunk, future = pending.popleft()
                try:
                    result = future.result()
                except RuntimeError:
                    journal.event("mapper_retry", chunk=number)
                    result = pool.submit(map_chunk, chunk, True).result()
                state.apply(number, result[0][1], selected=result[1], rows=len(chunk))
                if checkpoint_allowed:
                    pointer = save_checkpoint(args.work, epoch=token.epoch, state=state)
                    # Reconnection may create a new session, but never refreshes this leader's token.
                    reconnect(zk.sync, store.root + "/epoch")
                    try:
                        version = store.write_checkpoint(token, pointer)
                        journal.event("checkpoint", epoch=token.epoch, next_chunk=state.next_chunk,
                                      checkpoint_version=version, selected_rows=state.selected_rows,
                                      unique_pairs=len(state.counts))
                    except BadVersionError:
                        checkpoint_allowed = False
                        journal.event("checkpoint_rejected", epoch=token.epoch,
                                      next_chunk=state.next_chunk, error="BadVersionError")
                time.sleep(args.delay)
    path = args.work.resolve() / "candidates" / f"epoch-{token.epoch}-{args.id}.json"
    sha = atomic_json(path, pair_records(state.counts))
    pointer = {"path": str(path), "sha256": sha, "coordinator": args.id,
               "epoch": token.epoch, "source_sha256": state.source_sha256,
               "rows": state.rows, "selected_rows": state.selected_rows,
               "unique_pairs": len(state.counts), "pair_occurrences": sum(state.counts.values())}
    journal.event("result_ready", **{key: value for key, value in pointer.items() if key != "coordinator"})
    reconnect(zk.sync, store.root + "/epoch")
    return pointer


def run(args) -> None:
    journal = Journal(args.log, args.id)
    actual_sha = source_digest(args.source)
    expected_sha = json.loads((Path(__file__).resolve().parents[1] / "baseline/source.json").read_text())["sha256"]
    if actual_sha != expected_sha:
        raise ValueError("Source CSV SHA-256 differs from HW3")
    journal.event("source_verified", sha256=actual_sha, bytes=args.source.stat().st_size)
    wake = threading.Event()
    zk = KazooClient(hosts=args.hosts, timeout=10)

    def session_changed(state):
        journal.event("session_state", state=state)
        wake.set()

    zk.add_listener(session_changed)
    try:
        zk.start(timeout=15)
        journal.event("connected", requested_session_timeout_ms=10000,
                      configured_session_timeout_ms=zk._session_timeout)
        store = EpochStore(zk, args.root)
        config = {"source_sha256": actual_sha, "chunk_rows": args.chunk_rows}
        try:
            zk.create(store.root + "/config", json.dumps(config).encode())
        except NodeExistsError:
            pass
        if json.loads(zk.get(store.root + "/config")[0]) != config:
            raise ValueError("Run source or chunk configuration mismatch")
        while True:
            candidate = become_candidate(zk, store, journal, wake)
            if candidate is None:
                return
            KazooRetry(max_tries=-1, deadline=20)(zk.sync, store.root + "/epoch")
            try:
                token = store.acquire_epoch(candidate=candidate)
                break
            except NoNodeError:
                journal.event("promotion_rejected", reason="candidate session expired")
        pointer = store.checkpoint()
        state = (load_checkpoint(pointer, source_sha256=actual_sha, chunk_rows=args.chunk_rows)
                 if pointer else State(actual_sha, chunk_rows=args.chunk_rows))
        journal.event("leader", epoch=token.epoch, epoch_version=token.version,
                      next_chunk=state.next_chunk, selected_rows=state.selected_rows,
                      restored_from_epoch=pointer["epoch"] if pointer else None,
                      restored_checkpoint=pointer)
        pointer = calculate(args, zk, store, token, state, journal)
        try:
            version = store.publish(token, pointer, fencing=args.fencing)
            journal.event("published", epoch=token.epoch, pointer_version=version,
                          fencing=args.fencing, message="записал результат")
        except BadVersionError:
            journal.event("publication_rejected", epoch=token.epoch, fencing=args.fencing,
                          error="BadVersionError", message="BadVersionError: устаревшая эпоха, запись отклонена")
    except Exception as error:
        journal.event("failed", error=type(error).__name__, detail=str(error))
        raise
    finally:
        zk.stop()
        zk.close()
        journal.stream.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--hosts", required=True)
    parser.add_argument("--root", required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--chunk-rows", type=int, default=2000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--delay", type=float, default=0.12)
    parser.add_argument("--fencing", type=int, choices=[0, 1], default=int(os.getenv("FENCING", "0")))
    arguments = parser.parse_args()
    if arguments.chunk_rows <= 0 or arguments.workers <= 0 or arguments.delay < 0:
        parser.error("chunk-rows/workers must be positive; delay must be nonnegative")
    arguments.fencing = bool(arguments.fencing)
    run(arguments)
