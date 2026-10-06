import importlib
import runpy
import sys
from pathlib import Path

import pytest


def test_run_dim_date_seed_driver_invokes_seed_module(monkeypatch: pytest.MonkeyPatch) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    script_path = repo_root / "run_dim_date_seed.py"

    assert script_path.exists(), "Expected a dedicated driver script for dim_date seeding"

    calls: dict[str, object] = {}

    def fake_run_path(path: str, run_name: str | None = None) -> None:
        calls["path"] = path
        calls["run_name"] = run_name
        raise SystemExit(0)

    monkeypatch.setattr(runpy, "run_path", fake_run_path)
    monkeypatch.setattr(sys, "argv", ["run_dim_date_seed.py"])

    module = importlib.import_module("run_dim_date_seed")

    with pytest.raises(SystemExit) as exc_info:
        module.main()

    assert exc_info.value.code == 0
    assert calls["path"].endswith("etl/seed_dim_date.py")
    assert calls["run_name"] == "__main__"
