"""Consistent snapshots of the contiguous prefix already reduced by a leader."""

import hashlib
import json
import os
import tempfile
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class State:
    source_sha256: str
    chunk_rows: int = 2000
    next_chunk: int = 0
    selected_rows: int = 0
    rows: int = 0
    counts: Counter = field(default_factory=Counter)

    def apply(self, number: int, pairs: Counter, *, selected: int, rows: int = 0) -> None:
        if number != self.next_chunk:
            raise ValueError(f"Expected chunk {self.next_chunk}, received chunk {number}")
        self.counts.update(pairs)
        self.selected_rows += selected
        self.rows += rows
        self.next_chunk += 1


def atomic_json(path: Path, value: dict | list) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = (json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return hashlib.sha256(body).hexdigest()


def pair_records(counts: Counter) -> list[dict]:
    return [{"skills": list(pair), "count": count} for pair, count in sorted(counts.items())]


def save_checkpoint(directory: Path, *, epoch: int, state: State) -> dict:
    value = {"schema": 1, "source_sha256": state.source_sha256,
             "chunk_rows": state.chunk_rows, "next_chunk": state.next_chunk,
             "selected_rows": state.selected_rows, "rows": state.rows,
             "counts": pair_records(state.counts)}
    path = directory.resolve() / "checkpoints" / f"epoch-{epoch}" / f"chunk-{state.next_chunk:06d}.json"
    sha = atomic_json(path, value)
    return {"path": str(path), "sha256": sha, "epoch": epoch, "next_chunk": state.next_chunk}


def load_checkpoint(pointer: dict, *, source_sha256: str,
                    chunk_rows: int | None = None) -> State:
    body = Path(pointer["path"]).read_bytes()
    if hashlib.sha256(body).hexdigest() != pointer["sha256"]:
        raise ValueError("Checkpoint SHA-256 mismatch")
    value = json.loads(body)
    if value["schema"] != 1 or value["source_sha256"] != source_sha256:
        raise ValueError("Checkpoint schema or source mismatch")
    if chunk_rows is not None and value["chunk_rows"] != chunk_rows:
        raise ValueError("Checkpoint chunk_rows mismatch")
    return State(source_sha256=value["source_sha256"], chunk_rows=value["chunk_rows"],
                 next_chunk=value["next_chunk"], selected_rows=value["selected_rows"],
                 rows=value["rows"],
                 counts=Counter({tuple(row["skills"]): row["count"] for row in value["counts"]}))
