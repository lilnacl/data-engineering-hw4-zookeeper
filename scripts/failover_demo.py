"""Suspend a real coordinator and verify both failover and stale-writer behavior."""

import argparse
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from kazoo.client import KazooClient

ROOT = Path(__file__).resolve().parents[1]
HOSTS = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"


def events(path: Path) -> list[dict]:
    if not path.exists():
        return []
    result = []
    for line in path.read_text().splitlines():
        try:
            result.append(json.loads(line))
        except json.JSONDecodeError:
            continue  # The writer may be in the middle of its last append.
    return result


def wait_event(path: Path, name: str, process: subprocess.Popen, *, timeout: float = 45,
               minimum_chunk: int = 0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for event in events(path):
            if event["event"] == name and event.get("next_chunk", 0) >= minimum_chunk:
                return event
        if process.poll() is not None:
            raise RuntimeError(f"Coordinator exited ({process.returncode}) before {name}; see {path.parent}")
        time.sleep(0.05)
    raise TimeoutError(f"Missing {name}; see {path}")


def run_mode(source: Path, *, fencing: bool, pause_seconds: float) -> dict:
    mode = "with-fencing" if fencing else "without-fencing"
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + mode
    directory = ROOT / "logs" / "failover" / run_id
    directory.mkdir(parents=True)
    work = ROOT / "work" / run_id
    zk_root = "/hw4/runs/" + run_id
    processes = {}
    outputs = []
    stopped = False
    control = []
    print(f"Starting {mode}; evidence: {directory}", flush=True)
    try:
        for actor in ["A", "B"]:
            command = [sys.executable, "-m", "hw4.coordinator", "--id", actor,
                       "--hosts", HOSTS, "--root", zk_root, "--source", str(source),
                       "--work", str(work), "--log", str(directory / f"{actor}.log"),
                       "--delay", "0.12"]
            stream = (directory / f"{actor}-console.log").open("w")
            outputs.append(stream)
            processes[actor] = subprocess.Popen(command, cwd=ROOT,
                                                env=os.environ | {"FENCING": str(int(fencing))},
                                                stdout=stream, stderr=subprocess.STDOUT,
                                                start_new_session=True)
            control.append({"event": "started", "actor": actor, "pid": processes[actor].pid,
                            "command": command, "FENCING": int(fencing)})
            if actor == "A":
                wait_event(directory / "A.log", "leader", processes["A"])
            else:
                wait_event(directory / "B.log", "candidate", processes["B"])
        checkpoint = wait_event(directory / "A.log", "checkpoint", processes["A"], minimum_chunk=8)
        stopped_ns = time.monotonic_ns()
        os.kill(processes["A"].pid, signal.SIGSTOP)
        stopped = True
        control.append({"event": "SIGSTOP", "actor": "A", "monotonic_ns": stopped_ns,
                        "wall_time": datetime.now(timezone.utc).isoformat(),
                        "checkpoint_observed": checkpoint["next_chunk"]})
        leader_b = wait_event(directory / "B.log", "leader", processes["B"], timeout=45)
        elapsed = (leader_b["monotonic_ns"] - stopped_ns) / 1e9
        print(f"{mode}: B became leader after {elapsed:.3f}s; resumed at chunk {leader_b['next_chunk']}", flush=True)
        published_b = wait_event(directory / "B.log", "published", processes["B"], timeout=90)
        assert leader_b["next_chunk"] > 0, "Backup must load a nonempty checkpoint"
        assert leader_b["epoch"] > checkpoint["epoch"]
        while (time.monotonic_ns() - stopped_ns) / 1e9 < pause_seconds:
            time.sleep(0.1)
        actual_pause = (time.monotonic_ns() - stopped_ns) / 1e9
        os.kill(processes["A"].pid, signal.SIGCONT)
        stopped = False
        control.append({"event": "SIGCONT", "actor": "A", "monotonic_ns": time.monotonic_ns(),
                        "actual_pause_seconds": actual_pause})
        for actor, process in processes.items():
            process.wait(timeout=100)
            assert process.returncode == 0, f"{actor} exited {process.returncode}; see console log"
        journal = {actor: events(directory / f"{actor}.log") for actor in processes}
        successful = [e for lines in journal.values() for e in lines if e["event"] == "published"]
        rejected = [e for e in journal["A"] if e["event"] == "publication_rejected"]
        zk = KazooClient(hosts=HOSTS, timeout=10)
        zk.start(timeout=10)
        try:
            raw, stat = zk.get(zk_root + "/result")
            pointer = json.loads(raw)
            epoch_raw, epoch_stat = zk.get(zk_root + "/epoch")
            checkpoint_raw, checkpoint_stat = zk.get(zk_root + "/checkpoint")
        finally:
            zk.stop()
            zk.close()
        expected_count = 1 if fencing else 2
        assert len(successful) == expected_count, successful
        assert stat.version == expected_count, stat.version
        assert pointer["coordinator"] == ("B" if fencing else "A")
        if fencing:
            assert len(rejected) == 1 and rejected[0]["error"] == "BadVersionError"
        else:
            assert not rejected
        baseline = json.loads((ROOT / "baseline/counts_2.json").read_text())
        differences = {}
        candidates = [e for lines in journal.values() for e in lines if e["event"] == "result_ready"]
        assert len(candidates) == 2
        for candidate in candidates:
            body = Path(candidate["path"]).read_bytes()
            assert hashlib.sha256(body).hexdigest() == candidate["sha256"]
            records = json.loads(body)
            expected = {tuple(r["skills"]): r["count"] for r in baseline}
            actual = {tuple(r["skills"]): r["count"] for r in records}
            difference = sum(expected.get(key) != actual.get(key) for key in expected.keys() | actual.keys())
            differences[candidate["coordinator"]] = difference
            assert difference == 0
            assert candidate["selected_rows"] == 3123
            assert len(actual) == 13552 and sum(actual.values()) == 61694
        summary = {"run_id": run_id, "fencing": fencing, "zk_root": zk_root,
                   "session_timeout_ms": 10000, "leader_switch_seconds": elapsed,
                   "pause_seconds": actual_pause, "backup_resumed_next_chunk": leader_b["next_chunk"],
                   "backup_finished_before_resume": published_b["monotonic_ns"] < control[-1]["monotonic_ns"],
                   "published_count": len(successful), "pointer_version": stat.version,
                   "pointer": pointer, "epoch": int(epoch_raw), "epoch_version": epoch_stat.version,
                   "checkpoint_pointer": json.loads(checkpoint_raw), "checkpoint_version": checkpoint_stat.version,
                   "old_leader_error": rejected[0]["error"] if rejected else None,
                   "pair_differences_from_hw3": differences, "passed": True}
        (directory / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
        (directory / "control.json").write_text(json.dumps(control, ensure_ascii=False, indent=2) + "\n")
        print(f"{mode}: PASS; publications={len(successful)}, pointer version={stat.version}, differences={differences}", flush=True)
        return summary
    finally:
        if stopped and processes["A"].poll() is None:
            os.kill(processes["A"].pid, signal.SIGCONT)
        for process in processes.values():
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                process.wait(timeout=10)
        for stream in outputs:
            stream.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=ROOT / "work/it_vacancies_2024.csv")
    parser.add_argument("--pause-seconds", type=float, default=40)
    parser.add_argument("--mode", choices=["both", "0", "1"], default="both")
    args = parser.parse_args()
    for enabled in ([False, True] if args.mode == "both" else [args.mode == "1"]):
        run_mode(args.source.resolve(), fencing=enabled, pause_seconds=args.pause_seconds)
