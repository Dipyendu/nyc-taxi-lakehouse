"""scripts/ledger.sh: quota refusal, wall-clock limit and result column."""

import shutil
import subprocess
from pathlib import Path

import pytest

LEDGER = Path(__file__).resolve().parents[1] / "scripts" / "ledger.sh"
pytestmark = pytest.mark.skipif(shutil.which("timeout") is None, reason="needs the timeout command")


def run(tmp_path, quota_text, *cmd, limit="5"):
    quota, ledger = tmp_path / "QUOTA.log", tmp_path / "ledger.md"
    quota.write_text(quota_text)
    env = {"PATH": "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin", "TAXI_QUOTA_LOG": str(quota),
           "TAXI_RUN_LEDGER": str(ledger), "LEDGER_LIMIT_S": limit, "HOME": str(tmp_path)}
    p = subprocess.run(
        ["bash", str(LEDGER), "test run", "0 bytes", *cmd], check=False, env=env, capture_output=True, text=True
    )
    return p.returncode, (ledger.read_text().splitlines()[-1] if ledger.exists() else "")


def test_command_over_its_limit_is_killed_and_logged_as_timeout(tmp_path):
    rc, row = run(tmp_path, "START x\n", "sleep", "30", limit="1")
    assert rc == 124
    assert "TIMEOUT after 1 s" in row


def test_quota_stop_refuses_to_start(tmp_path):
    rc, row = run(tmp_path, "START x\nQUOTA-STOP 10:00 A quota\n", "true")
    assert rc == 75 and row == ""


def test_resume_after_stop_runs_and_skip_code_is_reported(tmp_path):
    rc, row = run(tmp_path, "QUOTA-STOP 10:00 A q\nQUOTA-RESUME 11:00 A\n", "bash", "-c", "exit 99")
    assert rc == 99 and "SKIP rc=99" in row


def test_guard_is_off_when_the_variable_is_unset(tmp_path):
    # A copy of the script inside a fake repository, with a QUOTA-STOP file next to it: unset means no guard,
    # so no file outside the repository is read.
    scripts = tmp_path / "repo" / "scripts"
    scripts.mkdir(parents=True)
    shutil.copy(LEDGER, scripts / "ledger.sh")
    (tmp_path / "QUOTA.log").write_text("QUOTA-STOP 10:00 A quota\n")
    ledger = tmp_path / "ledger.md"
    env = {"PATH": "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin", "TAXI_RUN_LEDGER": str(ledger),
           "LEDGER_LIMIT_S": "5", "HOME": str(tmp_path)}
    p = subprocess.run(["bash", str(scripts / "ledger.sh"), "test run", "0 bytes", "true"],
                       check=False, env=env, capture_output=True, text=True)
    assert p.returncode == 0 and ledger.read_text().splitlines()[-1].endswith("| OK |")
