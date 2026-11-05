"""Reproduce quorum loss and ephemeral-session cleanup on the HW4 ensemble."""

import json
import re
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SERVERS = ("zk1", "zk2", "zk3")


class Experiment:
    def __init__(self) -> None:
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.logs = ROOT / "logs" / "ensemble" / self.run_id
        self.logs.mkdir(parents=True)
        docker = shutil.which("docker") or str(Path.home() / ".docker/bin/docker")
        self.compose = [docker, "compose", "-f", str(ROOT / "compose.yaml")]
        self.namespace = f"/hw4-ensemble-{self.run_id}"

    def record(self, args: list[str], label: str, result: dict) -> None:
        with (self.logs / "commands.jsonl").open("a") as stream:
            stream.write(json.dumps({"command": args, "output": label + ".log", **result}) + "\n")

    def run(self, args: list[str], label: str, *, data: str | None = None,
            timeout: float = 30) -> tuple[int, str]:
        command = self.compose + args
        started = time.monotonic()
        try:
            result = subprocess.run(command, input=data, text=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                    timeout=timeout, check=False)
            code, output = result.returncode, result.stdout
            timed_out = False
        except subprocess.TimeoutExpired as error:
            code, timed_out = 124, True
            output = error.stdout or ""
            if isinstance(output, bytes):
                output = output.decode(errors="replace")
            output += f"\nExperiment harness: command exceeded {timeout} seconds.\n"
        (self.logs / f"{label}.log").write_text(output)
        self.record(command, label, {"exit_code": code, "timed_out": timed_out,
                                    "seconds": round(time.monotonic() - started, 3),
                                    "stdin": data})
        return code, output

    def cli_args(self, server: str) -> list[str]:
        return ["exec", "-T", "-e", "CLIENT_JVMFLAGS=-Dzookeeper.request.timeout=6000",
                server, "zkCli.sh", "-timeout", "10000", "-server", f"{server}:2181"]

    def cli(self, server: str, commands: list[str], label: str) -> tuple[int, str]:
        return self.run(self.cli_args(server), label,
                        data="\n".join(commands + ["quit", ""]), timeout=18)

    def snapshot(self, label: str) -> dict[str, str]:
        code, _ = self.run(["ps", "-a", "--format", "json"], f"{label}-containers")
        assert code == 0, "Cannot list containers"
        roles = {}
        for server in SERVERS:
            code, output = self.run(["exec", "-T", server, "zkServer.sh", "status"],
                                    f"{label}-{server}-status", timeout=10)
            match = re.search(r"Mode: (\w+)", output)
            roles[server] = match.group(1) if code == 0 and match else "unavailable"
        return roles

    def wait_for_output(self, path: Path, text: str, process: subprocess.Popen,
                        timeout: float = 15) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if text in path.read_text():
                return
            if process.poll() is not None:
                raise RuntimeError(f"Client exited before {text!r}; see {path}")
            time.sleep(0.1)
        raise TimeoutError(f"Did not observe {text!r}; see {path}")

    def ephemeral(self) -> dict:
        path = self.namespace + "/ephemeral"
        log_path = self.logs / "ephemeral-creator.log"
        command = self.compose + self.cli_args("zk1")
        with log_path.open("w") as log:
            creator = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=log,
                                       stderr=subprocess.STDOUT, text=True)
            try:
                self.wait_for_output(log_path, "state:SyncConnected", creator)
                creator.stdin.write(f"create -e {path} session-owned\n")
                creator.stdin.flush()
                self.wait_for_output(log_path, "Created " + path, creator)
                before_code, before = self.cli("zk2", [f"get -s {path}"], "ephemeral-before-close")
                assert before_code == 0 and "session-owned" in before
                owner = re.search(r"ephemeralOwner = (0x[0-9a-f]+)", before)
                assert owner and int(owner.group(1), 16) != 0, "Node must be ephemeral"
                creator.stdin.write("quit\n")
                creator.stdin.flush()
                creator.stdin.close()
                creator.wait(timeout=10)
                assert creator.returncode == 0, "Creator session did not close cleanly"
                after_code, after = self.cli("zk2", [f"get -s {path}"], "ephemeral-after-close")
                assert after_code == 1 and "Node does not exist" in after
                self.record(command, "ephemeral-creator", {
                    "exit_code": creator.returncode,
                    "stdin": f"create -e {path} session-owned\nquit\n"})
                return {"path": path, "owner": owner.group(1), "exists_before_close": True,
                        "missing_after_close": True, "creator_exit_code": creator.returncode,
                        "observer_after_exit_code": after_code}
            finally:
                if creator.poll() is None:
                    creator.terminate()
                    creator.wait(timeout=10)

    def execute(self) -> None:
        summary = {"run_id": self.run_id, "session_timeout_ms": 10000}
        print(f"Evidence: {self.logs}", flush=True)
        try:
            roles = self.snapshot("three-alive")
            assert sorted(roles.values()) == ["follower", "follower", "leader"], roles
            summary["three_alive_roles"] = roles
            followers = [server for server, role in roles.items() if role == "follower"]
            leader = next(server for server, role in roles.items() if role == "leader")
            code, output = self.cli(leader, [f"create {self.namespace} ensemble-demo"], "baseline-write")
            assert code == 0 and "Created " + self.namespace in output

            print(f"Stopping one server: {followers[0]}", flush=True)
            code, _ = self.run(["stop", followers[0]], "stop-first")
            assert code == 0
            code, output = self.cli(leader, [f"create {self.namespace}/proof quorum-survived",
                                            f"get {self.namespace}/proof"], "one-lost-write-read")
            assert code == 0 and "Created " in output and "quorum-survived" in output
            summary["one_lost"] = {"stopped": followers[:1], "write_read_ok": True,
                                    "roles": self.snapshot("two-alive")}
            print("Two servers: write and read succeeded", flush=True)

            print(f"Stopping second server: {followers[1]}", flush=True)
            code, _ = self.run(["stop", followers[1]], "stop-second")
            assert code == 0
            deadline = time.monotonic() + 35
            attempt = 0
            while True:
                attempt += 1
                code, _ = self.run(["exec", "-T", leader, "zkServer.sh", "status"],
                                    f"quorum-loss-status-{attempt}", timeout=10)
                if code != 0:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError("Remaining server still reports a serving role")
                time.sleep(0.5)
            code, output = self.cli(leader, [f"create {self.namespace}/without-quorum blocked"],
                                    "two-lost-write-fails")
            assert code != 0 and "Created " + self.namespace + "/without-quorum" not in output
            summary["two_lost"] = {"stopped": followers, "write_exit_code": code,
                                    "write_unavailable": True,
                                    "roles": self.snapshot("one-alive")}
            print(f"One server: write unavailable (exit {code})", flush=True)
        finally:
            print("Restoring all three servers", flush=True)
            code, output = self.run(["up", "-d", "--wait", "--wait-timeout", "120"],
                                    "restore", timeout=150)
            if code != 0:
                raise RuntimeError("Ensemble restore failed: " + output)

        summary["restored_roles"] = self.snapshot("restored")
        assert sorted(summary["restored_roles"].values()) == ["follower", "follower", "leader"]
        code, output = self.cli("zk1", [f"get {self.namespace}/proof"], "restored-read")
        assert code == 0 and "quorum-survived" in output
        summary["persistent_data_survived"] = True
        summary["ephemeral"] = self.ephemeral()
        print("Ephemeral node: observed by second client, absent after creator quit", flush=True)
        code, _ = self.cli("zk1", [f"deleteall {self.namespace}"], "cleanup-demo-namespace")
        assert code == 0
        code, _ = self.run(["logs", "--no-color", "--timestamps"], "servers", timeout=30)
        assert code == 0
        summary["passed"] = True
        (self.logs / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
        print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    Experiment().execute()
