#!/usr/bin/env python3
"""Own a foreground demo and clean up its model services on terminal loss."""

import errno
import fcntl
import os
from pathlib import Path
import secrets
import select
import signal
import subprocess
import sys


def say(message):
    try:
        print(message, flush=True)
    except OSError:
        pass  # The terminal may already be gone.


def open_terminal():
    try:
        return os.open('/dev/tty', os.O_RDONLY | os.O_NONBLOCK | os.O_NOCTTY)
    except OSError as error:
        if error.errno not in (errno.ENXIO, errno.ENODEV, errno.ENOTTY):
            raise
        return None


def run_hook(environment, name):
    command = environment.get(name)
    if not command:
        return 0
    with open('icd_demo_hook.out', 'ab') as log:
        # Finish the startup transaction even if its caller loses the PTY.
        return subprocess.call(
            ['/bin/sh', '-c', command], stdin=subprocess.DEVNULL, stdout=log,
            stderr=subprocess.STDOUT, start_new_session=True, env=environment,
        )


def stop_server(server, pidfile):
    if server is None:
        return
    if server.poll() is None:
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()
    try:
        if pidfile.read_text().strip() == str(server.pid):
            pidfile.unlink()
    except FileNotFoundError:
        pass


def is_running(pidfile):
    try:
        pid = int(pidfile.read_text())
        if pid <= 1:
            return False
        os.kill(pid, 0)
        return True
    except (FileNotFoundError, ValueError, ProcessLookupError):
        return False
    except PermissionError:
        return True


def run():
    os.umask(0o077)
    interrupted = False

    def request_stop(_signum, _frame):
        nonlocal interrupted
        interrupted = True

    for signum in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, request_stop)

    tty = open_terminal()
    try:
        watcher = select.poll()
        if tty is not None:
            watcher.register(tty, select.POLLHUP | select.POLLERR)

        def stopping():
            return interrupted or (tty is not None and bool(watcher.poll(0)))

        with open('.icd_demo_lifecycle.lock', 'a') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                say('다른 데모 실행이 진행 중입니다.')
                return 1
            pidfile = Path(os.environ['ICD_DEMO_PIDFILE'])
            if is_running(pidfile):
                say('이미 켜진 데모가 있습니다. 먼저 server/demo.sh stop으로 종료하세요.')
                return 1
            if stopping():
                return 0

            environment = dict(os.environ, ICD_DEMO_MODEL_OWNER=secrets.token_hex(16))
            server = None
            result = 1
            try:
                start_status = run_hook(environment, 'ICD_DEMO_MODEL_START_CMD')
                if stopping():
                    result = 0
                elif start_status:
                    say('모델 시작에 실패했습니다. server/icd_demo_hook.out을 확인하세요.')
                else:
                    with open('icd_demo_server.log', 'ab') as log:
                        server = subprocess.Popen(
                            [sys.executable, '-u', 'icd_demo_server.py'], stdin=subprocess.DEVNULL,
                            stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
                            env={**environment, 'ICD_DEMO_MODEL_STOP_CMD': ''},
                        )
                        while not stopping():
                            try:
                                status = server.wait(timeout=0.25)
                            except subprocess.TimeoutExpired:
                                continue
                            result = 0 if status in (0, 3) else 1
                            if status == 3:
                                say('판독문 요청이 없어 데모가 종료됐습니다.')
                            break
                        else:
                            result = 0
            finally:
                try:
                    stop_server(server, pidfile)
                finally:
                    if run_hook(environment, 'ICD_DEMO_MODEL_STOP_CMD'):
                        say('모델 정리에 실패했습니다. server/icd_demo_hook.out을 확인하세요.')
                        result = 1
            say('데모를 종료했습니다.' if result == 0 else '데모가 오류로 종료됐습니다.')
            return result
    finally:
        if tty is not None:
            os.close(tty)


if __name__ == '__main__':
    raise SystemExit(run())
