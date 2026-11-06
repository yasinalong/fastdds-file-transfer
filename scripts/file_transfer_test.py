#!/usr/bin/env python3
"""Verify generic file transfer with independent hashes and real DDS processes."""
import argparse
import hashlib
import os
import re
from pathlib import Path
import signal
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--preset", choices=("dev", "release"), default="dev")
args = parser.parse_args()
BINARY = ROOT / "out/apps" / args.preset / "bin/file_transfer"
RUN = ROOT / "out/tests/file_transfer" / (time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:6])
RUN.mkdir(parents=True)
DOMAIN = str(80 + os.getpid() % 100)


def digest(path):
    with path.open("rb") as stream:
        value = hashlib.sha256()
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
        return value.hexdigest()


def fixture(name, size):
    path = RUN / name
    block = bytes(range(256)) * 4096
    with path.open("wb") as stream:
        while size:
            count = min(len(block), size)
            stream.write(block[:count])
            size -= count
    return path


def start(name, command, stdin=None):
    log = RUN / (name + ".log")
    with log.open("w") as stream:
        process = subprocess.Popen(command, stdin=stdin, stdout=stream, stderr=subprocess.STDOUT)
    return process, log


def stop(process):
    if process and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)


def wait_for(predicate, process, log, timeout=15):
    deadline = time.monotonic() + timeout
    while not predicate():
        if process.poll() is not None or time.monotonic() >= deadline:
            raise AssertionError(log.read_text())
        time.sleep(0.02)


def pair(name, source, sender_options=(), receiver_options=(), error=None, idle=0, interrupt=None):
    directory = RUN / (name + "-received")
    directory.mkdir()
    destination = directory / source.name
    if name == "existing":
        destination.write_bytes(b"sentinel")
    channel = "file_" + uuid.uuid4().hex[:12]
    common = ["--domain", DOMAIN, "--channel", channel, "--timeout", "6"]
    receiver, rlog = start(name + "-receiver", [str(BINARY), "receive", "--output", str(directory),
                            *common, *receiver_options])
    sender = None
    try:
        wait_for(lambda: "LISTENING" in rlog.read_text(), receiver, rlog)
        if idle:
            time.sleep(idle)
            assert receiver.poll() is None, "Idle receiver must remain available"
        sender, slog = start(name + "-sender", [str(BINARY), "send", str(source), *common, "--discover-ms", "100", *sender_options])
        if interrupt:
            def has_partial():
                return any(p.stat().st_size > 0 for p in (directory / ".file_transfer").glob("*.part"))
            wait_for(has_partial, sender, slog)
            sender.send_signal(signal.SIGTERM if interrupt == "cancel" else signal.SIGKILL)
        sender_code = sender.wait(timeout=45)
        if interrupt:
            assert sender_code == (2 if interrupt == "cancel" else -signal.SIGKILL), slog.read_text()
            wait_for(lambda: ("CANCELLED" if interrupt == "cancel" else "Receive progress timeout") in rlog.read_text(), receiver, rlog)
            if interrupt == "crash":
                assert "Receive progress timeout" in rlog.read_text(), rlog.read_text()
            assert not destination.exists()
        elif error:
            assert sender_code == 1, (slog.read_text(), rlog.read_text())
            assert error in slog.read_text(), slog.read_text()
            if name == "existing":
                assert destination.read_bytes() == b"sentinel"
            else:
                assert not destination.exists()
        else:
            assert sender_code == 0, (slog.read_text(), rlog.read_text())
            assert destination.stat().st_size == source.stat().st_size
            assert digest(destination) == digest(source)
            assert "COMPLETED" in slog.read_text() and "SAVED" in rlog.read_text()
        partials = list((directory / ".file_transfer").glob("*.part"))
        if interrupt:
            assert len(partials) == 1 and partials[0].stat().st_size > 0, "Interrupted bytes must remain resumable"
        else:
            assert not partials, "Completed files must not retain partial data"
        assert receiver.poll() is None, "Receiver must keep listening after each task"
        print("PASS", name, flush=True)
    finally:
        stop(sender)
        stop(receiver)


def persistent_listener():
    directory = RUN / "persistent"
    channel = "persistent_" + uuid.uuid4().hex[:12]
    common = ["--domain", DOMAIN, "--topic", channel, "--timeout", "10"]
    receiver, rlog = start("persistent-receiver", [str(BINARY), "receive", "--output", str(directory), *common])
    senders = []
    try:
        wait_for(lambda: "LISTENING" in rlog.read_text(), receiver, rlog)
        sources = [fixture("first.dat", 262147), fixture("第二张 图片.png", 1048579),
                   fixture("concurrent-a.bin", 16777221), fixture("concurrent-b.bin", 16777223)]
        for index, source in enumerate(sources[:2]):
            process, log = start("sequential-" + str(index), [str(BINARY), "send", str(source), *common, "--discover-ms", "100"])
            senders.append(process)
            assert process.wait(timeout=25) == 0, log.read_text()
            assert digest(directory / source.name) == digest(source)
            time.sleep(2.2) # Exceed the old per-file linger; the same listener must survive.
            assert receiver.poll() is None, rlog.read_text()
        concurrent = []
        for index, source in enumerate(sources[2:]):
            process, log = start("concurrent-" + str(index), [str(BINARY), "send", str(source), *common, "--discover-ms", "100"])
            senders.append(process)
            concurrent.append((process, log, source))
        for process, log, source in concurrent:
            assert process.wait(timeout=35) == 0, log.read_text()
            assert digest(directory / source.name) == digest(source)
        assert receiver.poll() is None
        assert sum(p.is_file() for p in directory.iterdir()) == 4, list(directory.iterdir())
        print("PASS persistent-listener-sequential-concurrent-original-names", flush=True)
    finally:
        for sender in senders:
            stop(sender)
        stop(receiver)


def sender_presence():
    channel = "presence_" + uuid.uuid4().hex[:12]
    common = ["--domain", DOMAIN, "--topic", channel]
    receiver, rlog = start("presence-receiver", [str(BINARY), "receive", "--output", str(RUN / "presence"),
                            *common, "--idle-timeout", "6"])
    peers = []
    try:
        wait_for(lambda: "LISTENING" in rlog.read_text(), receiver, rlog)
        for index in range(2):
            peer, log = start("presence-peer-" + str(index),
                              [str(BINARY.with_name("file_transfer_presence_test")), DOMAIN, channel], stdin=subprocess.PIPE)
            peers.append(peer)
            wait_for(lambda: "ONLINE" in log.read_text(), peer, log)
        time.sleep(6.5)
        assert receiver.poll() is None, "Connected senders without file traffic must prevent idle timeout"
        peers[0].stdin.close()
        assert peers[0].wait(timeout=5) == 0
        time.sleep(6.5)
        assert receiver.poll() is None, "One remaining sender must keep receiver online"
        departed = time.monotonic()
        peers[1].stdin.close()
        assert peers[1].wait(timeout=5) == 0
        assert receiver.wait(timeout=16) == 0, rlog.read_text()
        assert time.monotonic() - departed >= 5.8, "Timer must start after last sender departs"
        assert "IDLE_TIMEOUT no sender present" in rlog.read_text()
        print("PASS sender-presence-last-departure-idle-timeout", flush=True)
    finally:
        for peer in peers:
            stop(peer)
            if peer.stdin and not peer.stdin.closed:
                peer.stdin.close()
        stop(receiver)


def initial_idle_timeout():
    receiver, log = start("initial-idle", [str(BINARY), "receive", "--output", str(RUN / "initial-idle"),
                         "--domain", DOMAIN, "--topic", "idle_" + uuid.uuid4().hex[:10], "--idle-timeout", "1"])
    try:
        assert receiver.wait(timeout=10) == 0, log.read_text()
        assert "IDLE_TIMEOUT no sender present" in log.read_text()
        print("PASS initial-no-sender-timeout", flush=True)
    finally:
        stop(receiver)


def broadcast():
    topic = "broadcast_" + uuid.uuid4().hex[:12]
    common = ["--domain", DOMAIN, "--topic", topic, "--timeout", "15"]
    source = fixture("broadcast.bin", 9 * 1024 * 1024 + 17)
    receivers = []
    sender = None
    try:
        for index in range(3):
            directory = RUN / ("broadcast-" + str(index))
            directory.mkdir()
            if index == 2:
                (directory / source.name).write_bytes(b"conflicting content")
            process, log = start("broadcast-receiver-" + str(index),
                                 [str(BINARY), "receive", "--output", str(directory), *common])
            receivers.append((process, log, directory))
            wait_for(lambda: "LISTENING" in log.read_text(), process, log)
        sender, log = start("broadcast-first", [str(BINARY), "send", str(source), *common,
                            "--receivers", "3", "--discover-ms", "100"])
        assert sender.wait(timeout=40) == 1, log.read_text()
        states = re.findall(r"^RECEIVER .*state=(\w+)", log.read_text(), re.MULTILINE)
        assert sorted(states) == ["COMPLETED", "COMPLETED", "FAILED"], log.read_text()
        for _, _, directory in receivers[:2]:
            assert digest(directory / source.name) == digest(source)
        assert (receivers[2][2] / source.name).read_bytes() == b"conflicting content"
        (receivers[2][2] / source.name).unlink() # Remove this test's conflicting fixture.
        sender, retry_log = start("broadcast-retry", [str(BINARY), "send", str(source), *common,
                                  "--receivers", "3", "--discover-ms", "100"])
        assert sender.wait(timeout=40) == 0, retry_log.read_text()
        resumed = [int(n) for n in re.findall(r"^RECEIVER .*resumed=(\d+)", retry_log.read_text(), re.MULTILINE)]
        assert sorted(resumed) == [0, source.stat().st_size, source.stat().st_size], retry_log.read_text()
        for _, _, directory in receivers:
            assert digest(directory / source.name) == digest(source)
        print("PASS broadcast-three-peers-partial-failure-idempotent-retry", flush=True)
    finally:
        stop(sender)
        for process, _, _ in receivers:
            stop(process)


def resume_after_restart(live_sender=False):
    name = "live-restart" if live_sender else "both-restart"
    directory = RUN / name
    topic = name + "_" + uuid.uuid4().hex[:10]
    common = ["--domain", DOMAIN, "--topic", topic, "--timeout", "30"]
    source = fixture(name + ".bin", 16 * 1024 * 1024 + 31)
    receiver, rlog = start(name + "-receiver-1", [str(BINARY), "receive", "--output", str(directory), *common])
    sender = replacement = None
    try:
        wait_for(lambda: "LISTENING" in rlog.read_text(), receiver, rlog)
        original_id = re.search(r"receiver=([0-9a-f]+)", rlog.read_text())[1]
        sender, slog = start(name + "-sender-1", [str(BINARY), "send", str(source), *common,
                             "--chunk-bytes", "1024", "--discover-ms", "100"])
        partial = directory / ".file_transfer" / (digest(source) + ".part")
        wait_for(lambda: partial.exists() and partial.stat().st_size >= 16384, sender, slog)
        receiver.kill()
        receiver.wait(timeout=5)
        if not live_sender:
            sender.kill()
            sender.wait(timeout=5)
        saved = partial.stat().st_size
        assert 0 < saved < source.stat().st_size
        replacement, replacement_log = start(name + "-receiver-2", [str(BINARY), "receive", "--output", str(directory), *common])
        wait_for(lambda: "LISTENING" in replacement_log.read_text(), replacement, replacement_log)
        assert "receiver=" + original_id in replacement_log.read_text(), replacement_log.read_text()
        if not live_sender:
            sender, slog = start(name + "-sender-2", [str(BINARY), "send", str(source), *common, "--discover-ms", "100"])
        assert sender.wait(timeout=60) == 0, slog.read_text()
        resumed = int(re.search(r"^RECEIVER .*resumed=(\d+)", slog.read_text(), re.MULTILINE)[1])
        assert resumed == saved, (saved, resumed, slog.read_text())
        assert digest(directory / source.name) == digest(source)
        assert not partial.exists()
        print("PASS", name + "-hash-prefix-resume", flush=True)
    finally:
        stop(sender)
        stop(receiver)
        stop(replacement)


def isolated_resume(live_restart=False):
    name = "isolated-restart" if live_restart else "isolated-prefixes"
    topic = name + "_" + uuid.uuid4().hex[:10]
    common = ["--domain", DOMAIN, "--topic", topic, "--timeout", "30"]
    source = fixture(name + ".bin", (64 if live_restart else 9) * 1024 * 1024 + 17)
    checksum = digest(source)
    peers = []
    sender = replacement = None
    prefixes = [0, 0, 0] if live_restart else [0, 12345, 12345, 2 * 1024 * 1024 + 7]
    try:
        for index, prefix in enumerate(prefixes):
            directory = RUN / (name + "-" + str(index))
            if prefix:
                cache = directory / ".file_transfer"
                cache.mkdir(parents=True)
                with source.open("rb") as stream:
                    (cache / (checksum + ".part")).write_bytes(stream.read(prefix))
            process, log = start(name + "-receiver-" + str(index),
                                 [str(BINARY), "receive", "--output", str(directory), *common])
            peers.append((process, log, directory))
            wait_for(lambda: "LISTENING" in log.read_text(), process, log)
        sender, slog = start(name + "-sender", [str(BINARY), "send", str(source), *common,
                            "--receivers", str(len(prefixes)), "--discover-ms", "100", "--chunk-bytes", "4096"])
        if live_restart:
            process, log, directory = peers[2]
            partial = directory / ".file_transfer" / (checksum + ".part")
            wait_for(lambda: partial.exists() and partial.stat().st_size >= 16384, sender, slog)
            process.kill()
            process.wait(timeout=5)
            prefixes[2] = partial.stat().st_size
            assert 0 < prefixes[2] < source.stat().st_size
            replacement, replacement_log = start(name + "-replacement", [str(BINARY), "receive", "--output", str(directory), *common])
        assert sender.wait(timeout=100) == 0, slog.read_text()
        for index, (_, log, directory) in enumerate(peers):
            assert digest(directory / source.name) == checksum
            if live_restart and index == 2:
                log = replacement_log
            text = log.read_text()
            rows = re.findall(r"DATA_STATS .*route=(normal|repair).*bytes=(\d+)", text)
            expected_route = "repair" if prefixes[index] else "normal"
            assert rows and all(route == expected_route for route, _ in rows), text
            assert sum(int(size) for _, size in rows) == source.stat().st_size - prefixes[index], text
        assert "route=repair" not in peers[0][1].read_text(), peers[0][1].read_text()
        if not live_restart:
            sessions = [re.search(r"DATA_OPEN .*route=repair session=([0-9a-f]+)", peers[i][1].read_text())[1]
                        for i in (1, 2, 3)]
            assert sessions[0] == sessions[1] and sessions[0] != sessions[2], sessions
        print("PASS", name + "-target-only-payload", flush=True)
    finally:
        stop(sender)
        stop(replacement)
        for process, _, _ in peers:
            stop(process)


def late_joiners():
    common = ["--domain", DOMAIN, "--topic", "late_" + uuid.uuid4().hex[:10], "--timeout", "30"]
    source = fixture("late-join.bin", 64 * 1024 * 1024 + 17)
    checksum = digest(source)
    peers = []
    sender = None
    paused = []
    try:
        directory = RUN / "late-original"
        original, log = start("late-original", [str(BINARY), "receive", "--output", str(directory), *common])
        peers.append((original, log, directory))
        wait_for(lambda: "LISTENING" in log.read_text(), original, log)
        sender, slog = start("late-sender", [str(BINARY), "send", str(source), *common,
                             "--receivers", "1", "--discover-ms", "100", "--chunk-bytes", "1024"])
        partial = directory / ".file_transfer" / (checksum + ".part")
        wait_for(lambda: partial.exists() and partial.stat().st_size >= 16384, sender, slog)
        # 暂停正常 Reader 制造有界背压，给两个真实新进程确定的发现时间。
        # 发送进程继续运行，正常协议不需要 STATUS 或暂停指令。
        original.send_signal(signal.SIGSTOP)
        paused.append(original)
        for index in range(2):
            directory = RUN / ("late-new-" + str(index))
            process, log = start("late-new-" + str(index), [str(BINARY), "receive", "--output", str(directory), *common])
            peers.append((process, log, directory))
            wait_for(lambda: "LISTENING" in log.read_text(), process, log)
        time.sleep(2)
        assert sender.poll() is None, slog.read_text()
        # 新端已上线，但尚未被本轮 Prepare 选中：只参与控制通信，不订阅正在发送的文件。
        for process, log, directory in peers[1:]:
            assert process.poll() is None, log.read_text()
            assert "DATA_OPEN" not in log.read_text(), "Waiting receiver opened a data Reader: " + log.read_text()
            assert not list((directory / ".file_transfer").glob("*.part")), "Waiting receiver created a partial file"
            assert not (directory / source.name).exists(), "Waiting receiver received the current file"
        original.send_signal(signal.SIGCONT)
        paused.clear()
        assert sender.wait(timeout=100) == 0, slog.read_text()
        assert len(re.findall(r"^RECEIVER ", slog.read_text(), re.MULTILINE)) == 3, slog.read_text()
        sessions = []
        for index, (_, log, directory) in enumerate(peers):
            assert digest(directory / source.name) == checksum
            rows = re.findall(r"DATA_STATS .*route=(normal|repair) session=([0-9a-f]+) bytes=(\d+)", log.read_text())
            route = "normal" if index == 0 else "repair"
            assert len(rows) == 1 and rows[0][0] == route and int(rows[0][2]) == source.stat().st_size, log.read_text()
            if index:
                sessions.append(rows[0][1])
        assert sessions[0] == sessions[1], sessions
        print("PASS late-joiners-control-only-while-waiting-and-shared-replay", flush=True)
    finally:
        for process in paused:
            if process.poll() is None:
                process.send_signal(signal.SIGCONT)
        stop(sender)
        for process, _, _ in peers:
            stop(process)


try:
    core = subprocess.run([str(BINARY.with_name("file_transfer_core_test")), str(RUN / "core")],
                          capture_output=True, text=True, timeout=30)
    (RUN / "core.log").write_text(core.stdout + core.stderr)
    assert core.returncode == 0, core.stdout + core.stderr
    print(core.stdout.strip(), flush=True)
    transport = subprocess.run([str(BINARY.with_name("file_transfer_transport_test")), DOMAIN],
                               capture_output=True, text=True, timeout=60)
    (RUN / "transport.log").write_text(transport.stdout + transport.stderr)
    assert transport.returncode == 0, transport.stdout + transport.stderr
    print(transport.stdout.strip().splitlines()[-1], flush=True)
    persistent_listener()
    sender_presence()
    initial_idle_timeout()
    broadcast()
    resume_after_restart()
    resume_after_restart(live_sender=True)
    isolated_resume()
    isolated_resume(live_restart=True)
    late_joiners()
    pair("empty", fixture("empty.bin", 0))
    pair("one-byte", fixture("one.bin", 1))
    pair("partial-last-chunk", fixture("arbitrary.png", 262144 + 7))
    large = fixture("large.bin", 64 * 1024 * 1024 + 37)
    pair("large", large)
    small = fixture("small.bin", 65539)
    pair("small-chunks", small, sender_options=["--chunk-bytes", "1024"])
    pair("idle-listener", small, receiver_options=["--timeout", "1"], idle=1.5)
    pair("oversize", small, receiver_options=["--max-bytes", "1"], error="File exceeds receiver limit")
    pair("existing", small, error="Destination already exists")
    for mode in ("cancel", "crash"):
        pair(mode, large, sender_options=["--chunk-bytes", "1024"],
             receiver_options=["--timeout", "2"], interrupt=mode)
    alone = subprocess.run([str(BINARY), "send", str(small), "--domain", DOMAIN,
                            "--channel", "absent_" + uuid.uuid4().hex[:12], "--timeout", "1"],
                           capture_output=True, text=True, timeout=10)
    (RUN / "no-peer.log").write_text(alone.stdout + alone.stderr)
    assert alone.returncode == 1 and "Discovery timeout" in alone.stderr, alone.stderr
    print("PASS no-peer", flush=True)
    print("PASS all file transfer checks; logs:", RUN, flush=True)
except Exception:
    print("FAIL; logs:", RUN, flush=True)
    raise
