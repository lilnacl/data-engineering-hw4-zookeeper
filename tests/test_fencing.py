import uuid

import pytest
from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError, NoNodeError


@pytest.fixture
def store():
    from hw4.coordination import EpochStore

    zk = KazooClient(hosts="127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183", timeout=10)
    zk.start(timeout=10)
    root = "/hw4-tests/" + uuid.uuid4().hex
    try:
        yield EpochStore(zk, root)
    finally:
        if zk.exists(root):
            zk.delete(root, recursive=True)
        zk.stop()
        zk.close()


def test_epochs_increase(store):
    first = store.acquire_epoch()
    second = store.acquire_epoch()
    assert (first.epoch, second.epoch) == (1, 2)
    assert second.version > first.version


def test_fencing_rejects_old_leader_even_before_new_result_is_published(store):
    old = store.acquire_epoch()
    store.acquire_epoch()
    with pytest.raises(BadVersionError):
        store.publish(old, {"coordinator": "A"}, fencing=True)
    assert store.result()[0] is None


def test_fencing_preserves_backup_result(store):
    old = store.acquire_epoch()
    current = store.acquire_epoch()
    assert store.publish(current, {"coordinator": "B"}, fencing=True) == 1
    with pytest.raises(BadVersionError):
        store.publish(old, {"coordinator": "A"}, fencing=True)
    pointer, version = store.result()
    assert pointer == {"coordinator": "B"}
    assert version == 1


def test_without_fencing_resumed_old_leader_overwrites_result(store):
    old = store.acquire_epoch()
    current = store.acquire_epoch()
    assert store.publish(current, {"coordinator": "B"}, fencing=False) == 1
    assert store.publish(old, {"coordinator": "A"}, fencing=False) == 2
    assert store.result() == ({"coordinator": "A"}, 2)


def test_old_leader_cannot_overwrite_shared_checkpoint(store):
    old = store.acquire_epoch()
    current = store.acquire_epoch()
    store.write_checkpoint(current, {"path": "B-checkpoint"})
    with pytest.raises(BadVersionError):
        store.write_checkpoint(old, {"path": "A-checkpoint"})
    assert store.checkpoint() == {"path": "B-checkpoint"}


def test_election_returns_ephemeral_candidate_path(store, tmp_path):
    import threading
    from hw4.coordinator import Journal, become_candidate

    journal = Journal(tmp_path / "candidate.log", "test")
    try:
        path = become_candidate(store.client, store, journal, threading.Event())
        assert isinstance(path, str)
        assert store.client.exists(path).ephemeralOwner == store.client.client_id[0]
    finally:
        journal.stream.close()


def test_expired_candidate_cannot_claim_a_new_epoch(store):
    candidate = store.client.create(store.root + "/election/candidate-", b"A",
                                    ephemeral=True, sequence=True)
    current = store.acquire_epoch()
    store.client.delete(candidate)
    with pytest.raises(NoNodeError):
        store.acquire_epoch(candidate=candidate)
    data, stat = store.client.get(store.root + "/epoch")
    assert int(data) == current.epoch
    assert stat.version == current.version
