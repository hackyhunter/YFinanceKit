#!/usr/bin/env python3
"""Run the existing parity harness across a broader Yahoo symbol/interval matrix.

This script deliberately orchestrates the canonical ``parity_harness.py`` rather
than reimplementing comparison logic. It is intended for manual/local validation
before advancing a YFinanceKit release or the nommminal package pin.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class Scenario:
    name: str
    symbols: tuple[str, ...]
    period: str
    interval: str


SCENARIOS: tuple[Scenario, ...] = (
    Scenario(
        "us-equities-daily",
        ("AAPL", "MSFT", "NVDA", "TSLA", "BRK-B"),
        "1mo",
        "1d",
    ),
    Scenario(
        "us-etfs-funds",
        ("SPY", "VOO", "QQQ", "IWM", "VTSAX"),
        "1mo",
        "1d",
    ),
    Scenario(
        "intraday-us",
        ("AAPL", "MSFT", "NVDA", "SPY"),
        "5d",
        "1h",
    ),
    Scenario(
        "uk-subunit",
        ("VOD.L", "BP.L", "HSBA.L", "SHEL.L"),
        "3mo",
        "1d",
    ),
    Scenario(
        "europe",
        ("ASML.AS", "OR.PA", "NESN.SW", "SAP.DE"),
        "1mo",
        "1d",
    ),
    Scenario(
        "asia-pacific",
        ("7203.T", "9984.T", "BHP.AX", "RELIANCE.NS"),
        "1mo",
        "1d",
    ),
    Scenario(
        "other-subunit-markets",
        ("NPN.JO", "SOL.JO", "TEVA.TA"),
        "3mo",
        "1d",
    ),
    Scenario(
        "crypto-fx",
        ("BTC-USD", "ETH-USD", "EURUSD=X", "JPY=X"),
        "1mo",
        "1d",
    ),
    Scenario(
        "indices",
        ("^GSPC", "^DJI", "^FTSE", "^N225"),
        "1mo",
        "1d",
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run cross-market YFinanceKit parity scenarios")
    parser.add_argument(
        "--package-path",
        default=str(Path(__file__).resolve().parents[1]),
        help="YFinanceKit package root",
    )
    parser.add_argument(
        "--scenario",
        action="append",
        default=[],
        help="Scenario name to run. Repeat to select several. Default: all.",
    )
    parser.add_argument(
        "--output-dir",
        default="artifacts/parity-matrix",
        help="Directory for scenario reports and aggregate JSON",
    )
    parser.add_argument("--timeout-sec", type=int, default=180)
    return parser.parse_args()


def selected_scenarios(names: Iterable[str]) -> list[Scenario]:
    wanted = {name.strip() for name in names if name.strip()}
    if not wanted:
        return list(SCENARIOS)
    known = {scenario.name: scenario for scenario in SCENARIOS}
    missing = sorted(wanted - known.keys())
    if missing:
        raise SystemExit(f"Unknown scenario(s): {', '.join(missing)}")
    return [known[name] for name in sorted(wanted)]


def main() -> int:
    args = parse_args()
    package = Path(args.package_path).resolve()
    harness = package / "tools" / "parity_harness.py"
    out_dir = Path(args.output_dir)
    if not out_dir.is_absolute():
        out_dir = package / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    scenarios = selected_scenarios(args.scenario)
    aggregate: dict[str, object] = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "package_path": str(package),
        "scenarios": [],
    }
    overall_rc = 0

    for scenario in scenarios:
        json_path = out_dir / f"{scenario.name}.json"
        md_path = out_dir / f"{scenario.name}.md"
        command = [
            sys.executable,
            str(harness),
            "--package-path",
            str(package),
            "--symbols",
            ",".join(scenario.symbols),
            "--period",
            scenario.period,
            "--interval",
            scenario.interval,
            "--output-json",
            str(json_path),
            "--output-md",
            str(md_path),
            "--timeout-sec",
            str(max(20, args.timeout_sec)),
        ]
        print(f"\n=== {scenario.name} ===")
        proc = subprocess.run(command, check=False)
        overall_rc = max(overall_rc, proc.returncode)

        summary: dict[str, object] | None = None
        if json_path.exists():
            try:
                report = json.loads(json_path.read_text(encoding="utf-8"))
                summary = report.get("summary")
            except (OSError, json.JSONDecodeError):
                summary = None

        aggregate["scenarios"].append(
            {
                "name": scenario.name,
                "symbols": list(scenario.symbols),
                "period": scenario.period,
                "interval": scenario.interval,
                "return_code": proc.returncode,
                "summary": summary,
                "json": str(json_path.relative_to(package)),
                "markdown": str(md_path.relative_to(package)),
            }
        )

    aggregate_path = out_dir / "aggregate.json"
    aggregate_path.write_text(json.dumps(aggregate, indent=2, sort_keys=True), encoding="utf-8")
    print(f"\nAggregate: {aggregate_path}")
    return overall_rc


if __name__ == "__main__":
    raise SystemExit(main())
