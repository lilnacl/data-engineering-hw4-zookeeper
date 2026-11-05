from collections import Counter
from pathlib import Path

import pytest


def test_checkpoint_resume_keeps_counts_without_replaying_committed_chunks(tmp_path):
    from hw4.checkpoint import State, load_checkpoint, save_checkpoint

    state = State("source-a")
    state.apply(0, Counter({("go", "sql"): 2}), selected=2)
    pointer = save_checkpoint(tmp_path, epoch=1, state=state)
    resumed = load_checkpoint(pointer, source_sha256="source-a")
    resumed.apply(1, Counter({("docker", "go"): 1}), selected=1)
    assert resumed.next_chunk == 2
    assert resumed.selected_rows == 3
    assert resumed.counts == Counter({("go", "sql"): 2, ("docker", "go"): 1})


@pytest.mark.parametrize("next_number", [0, 2])
def test_checkpoint_rejects_repeated_or_skipped_chunk(next_number):
    from hw4.checkpoint import State

    state = State("source-a")
    state.apply(0, Counter({("go", "sql"): 1}), selected=1)
    with pytest.raises(ValueError, match="chunk"):
        state.apply(next_number, Counter({("go", "sql"): 1}), selected=1)
    assert state.next_chunk == 1
    assert state.counts[("go", "sql")] == 1


def test_corrupt_checkpoint_is_rejected(tmp_path):
    from hw4.checkpoint import State, load_checkpoint, save_checkpoint

    pointer = save_checkpoint(tmp_path, epoch=1, state=State("source-a"))
    path = Path(pointer["path"])
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ValueError, match="SHA-256"):
        load_checkpoint(pointer, source_sha256="source-a")


def test_checkpoint_from_other_dataset_is_rejected(tmp_path):
    from hw4.checkpoint import State, load_checkpoint, save_checkpoint

    pointer = save_checkpoint(tmp_path, epoch=1, state=State("source-a"))
    with pytest.raises(ValueError, match="source"):
        load_checkpoint(pointer, source_sha256="source-b")


def test_checkpoint_with_other_chunk_size_is_rejected(tmp_path):
    from hw4.checkpoint import State, load_checkpoint, save_checkpoint

    pointer = save_checkpoint(tmp_path, epoch=1, state=State("source-a", chunk_rows=2))
    with pytest.raises(ValueError, match="chunk_rows"):
        load_checkpoint(pointer, source_sha256="source-a", chunk_rows=3)
