"""Epoch checks are performed atomically by the receiver of pointer updates."""

import json
from dataclasses import dataclass

from kazoo.exceptions import BadVersionError, NodeExistsError


@dataclass(frozen=True)
class Token:
    epoch: int
    version: int


class EpochStore:
    def __init__(self, client, root: str) -> None:
        self.client = client
        self.root = root.rstrip("/")
        client.ensure_path(self.root + "/election")
        for name, initial in [("epoch", b"0"), ("checkpoint", b""), ("result", b"")]:
            try:
                client.create(self.root + "/" + name, initial)
            except NodeExistsError:
                pass

    def acquire_epoch(self, candidate: str | None = None) -> Token:
        while True:
            data, stat = self.client.get(self.root + "/epoch")
            epoch = int(data) + 1
            try:
                if candidate is None:
                    new_stat = self.client.set(self.root + "/epoch", str(epoch).encode(), version=stat.version)
                else:
                    transaction = self.client.transaction()
                    transaction.check(candidate, version=0)
                    transaction.set_data(self.root + "/epoch", str(epoch).encode(), version=stat.version)
                    results = transaction.commit()
                    for result in results:
                        if isinstance(result, Exception):
                            raise result
                    new_stat = results[-1]
                return Token(epoch, new_stat.version)
            except BadVersionError:
                continue

    def _read(self, name: str) -> tuple[dict | None, int]:
        data, stat = self.client.get(self.root + "/" + name)
        return (json.loads(data) if data else None), stat.version

    def checkpoint(self) -> dict | None:
        return self._read("checkpoint")[0]

    def result(self) -> tuple[dict | None, int]:
        return self._read("result")

    def _checked_write(self, name: str, token: Token, value: dict) -> int:
        transaction = self.client.transaction()
        transaction.check(self.root + "/epoch", version=token.version)
        transaction.set_data(self.root + "/" + name, json.dumps(value).encode())
        results = transaction.commit()
        for result in results:
            if isinstance(result, Exception):
                raise result
        return results[-1].version

    def write_checkpoint(self, token: Token, pointer: dict) -> int:
        return self._checked_write("checkpoint", token, pointer)

    def publish(self, token: Token, pointer: dict, *, fencing: bool) -> int:
        if fencing:
            return self._checked_write("result", token, pointer)
        stat = self.client.set(self.root + "/result", json.dumps(pointer).encode(), version=-1)
        return stat.version
