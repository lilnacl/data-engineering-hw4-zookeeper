"""Display excerpts of saved experiment logs for terminal screenshots."""

import argparse
import json
import shlex
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENSEMBLE = Path("logs/ensemble/20261105T092042Z")


def command(*args):
    print("$ " + shlex.join(str(arg) for arg in args), flush=True)
    subprocess.run([str(arg) for arg in args], cwd=ROOT, check=True)
    print()


def excerpt(filename, terms):
    path = ENSEMBLE / filename
    pattern = "|".join(terms)
    command("grep", "-E", pattern, path)


def run_events(fencing):
    name = ("20261105T095153Z-with-fencing" if fencing
            else "20261105T095054Z-without-fencing")
    directory = ROOT / "logs/failover" / name
    for actor, event in [
        ("B", "leader"),
        ("B", "published"),
        ("A", "publication_rejected" if fencing else "published"),
    ]:
        records = [json.loads(line) for line in
                   (directory / f"{actor}.log").read_text().splitlines()]
        row = next(record for record in records if record["event"] == event)
        keys = ("time", "coordinator", "event", "epoch", "next_chunk",
                "restored_from_epoch", "pointer_version", "error", "message")
        print(f"{actor}.log:")
        print(json.dumps({key: row[key] for key in keys if key in row},
                         ensure_ascii=False))
        print()

    summary = json.loads((directory / "summary.json").read_text())
    print(f"{directory.relative_to(ROOT)}/summary.json:")
    for key in ("session_timeout_ms", "leader_switch_seconds", "pause_seconds",
                "backup_resumed_next_chunk", "backup_finished_before_resume",
                "published_count", "pointer_version", "old_leader_error",
                "pair_differences_from_hw3", "passed"):
        print(f"{key} = {json.dumps(summary[key], ensure_ascii=False)}")
    print("pointer = " + json.dumps({
        key: summary["pointer"][key] for key in
        ("coordinator", "epoch", "selected_rows", "unique_pairs", "pair_occurrences")
    }, ensure_ascii=False))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--panel", type=int, choices=range(1, 6), required=True)
    panel = parser.parse_args().panel
    if panel == 1:
        for node in ("zk1", "zk2", "zk3"):
            command("cat", ENSEMBLE / f"three-alive-{node}-status.log")
    elif panel == 2:
        for node in ("zk2", "zk3"):
            excerpt(f"two-alive-{node}-status.log", ["Mode:"])
        excerpt("one-lost-write-read.log", ["Created /", "quorum-survived"])
        excerpt("one-alive-zk3-status.log", ["Error contacting service"])
        excerpt("two-lost-write-fails.log", ["KeeperErrorCode"])
    elif panel == 3:
        excerpt("ephemeral-creator.log", ["Created /"])
        excerpt("ephemeral-before-close.log", ["session-owned", "ephemeralOwner"])
        excerpt("ephemeral-after-close.log", ["Node does not exist:"])
    else:
        mode = "1" if panel == 5 else "0"
        print(f"FENCING={mode}; selected fields from saved A.log, B.log, summary.json")
        print()
        run_events(panel == 5)


if __name__ == "__main__":
    main()
