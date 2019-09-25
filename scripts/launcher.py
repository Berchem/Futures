import os
from subprocess import call

import psutil

ROOT_DIR = os.path.dirname(__file__)
LOCKFILE = os.path.join(ROOT_DIR, "ProcessID")
OUT_FILE = os.path.join(ROOT_DIR, "launch.out")


def stdin(filename):
    with open(filename) as f:
        process_id = f.readline()
    return process_id


def stdout(filename, msg):
    with open(filename, "a") as out:
        out.write(msg + "\n")


def call_with_output(filename, cmd):
    with open(filename, "a") as out:
        call(cmd, stdout=out)


if os.path.isfile(LOCKFILE):
    stdout(OUT_FILE, "Lockfile Exists")
    pid = int(stdin(LOCKFILE))
    has_pid = pid not in psutil.pids()

    if has_pid:
        stdout(OUT_FILE, "The process [%s] is not running, resuming normal operation!" % pid)
        os.remove(LOCKFILE)

    else:
        stdout(OUT_FILE, "The process [%s] is already running" % pid)
        exit(1)

stdout(LOCKFILE, str(os.getpid()))

call_with_output(OUT_FILE, ["python", os.path.join(ROOT_DIR, "my_script.py")])

os.remove(LOCKFILE)
