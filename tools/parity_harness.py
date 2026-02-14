#!/usr/bin/env python3
"""
Swift YFinanceKit vs Python yfinance parity harness.

Runs a normalized snapshot comparison per symbol and writes:
  - JSON report (machine-readable)
  - Markdown report (quick scan)
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import yfinance as yf


DEFAULT_SYMBOLS = ["AAPL", "MSFT", "NVDA", "TSLA", "VOO", "BTC-USD"]
STATUS_ORDER = {"pass": 0, "warn": 1, "fail": 2, "skip": 3}


@dataclass
class CompareResult:
    status: str
    summary: str
    metrics: Dict[str, Any]
    issues: List[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Swift/Python yfinance parity checks.")
    parser.add_argument(
        "--package-path",
        default=str(Path(__file__).resolve().parents[1]),
        help="Path to Swift package root (default: parent of this script).",
    )
    parser.add_argument(
        "--symbols",
        default=",".join(DEFAULT_SYMBOLS),
        help="Comma-separated symbols (default: AAPL,MSFT,NVDA,TSLA,VOO,BTC-USD).",
    )
    parser.add_argument("--period", default="1mo", help="History period (default: 1mo).")
    parser.add_argument("--interval", default="1d", help="History interval (default: 1d).")
    parser.add_argument("--history-limit", type=int, default=30, help="History bars to compare.")
    parser.add_argument("--earnings-limit", type=int, default=4, help="Earnings rows to compare.")
    parser.add_argument("--income-limit", type=int, default=4, help="Income rows to compare.")
    parser.add_argument("--income-freq", default="yearly", choices=["yearly", "quarterly"], help="Income frequency.")
    parser.add_argument(
        "--output-json",
        default="artifacts/parity_report.json",
        help="JSON report output path (relative to package path if not absolute).",
    )
    parser.add_argument(
        "--output-md",
        default="artifacts/parity_report.md",
        help="Markdown report output path (relative to package path if not absolute).",
    )
    parser.add_argument("--swift-bin", default="swift", help="Swift binary (default: swift).")
    parser.add_argument("--timeout-sec", type=int, default=120, help="Per Swift snapshot timeout.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    package_path = Path(args.package_path).resolve()
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        print("No symbols provided.", file=sys.stderr)
        return 2

    output_json = resolve_output_path(package_path, args.output_json)
    output_md = resolve_output_path(package_path, args.output_md)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_md.parent.mkdir(parents=True, exist_ok=True)

    report = run_harness(
        package_path=package_path,
        symbols=symbols,
        swift_bin=args.swift_bin,
        period=args.period,
        interval=args.interval,
        history_limit=max(1, args.history_limit),
        earnings_limit=max(1, args.earnings_limit),
        income_limit=max(1, args.income_limit),
        income_freq=args.income_freq,
        timeout_sec=max(20, args.timeout_sec),
    )

    output_json.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    output_md.write_text(render_markdown(report), encoding="utf-8")

    summary = report["summary"]
    print(
        f"Parity complete: pass={summary['pass']} warn={summary['warn']} "
        f"fail={summary['fail']} skip={summary['skip']} score={summary['score']:.1f}"
    )
    print(f"JSON: {output_json}")
    print(f"MD:   {output_md}")
    return 0 if summary["fail"] == 0 else 1


def resolve_output_path(package_path: Path, raw: str) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path
    return package_path / path


def run_harness(
    *,
    package_path: Path,
    symbols: List[str],
    swift_bin: str,
    period: str,
    interval: str,
    history_limit: int,
    earnings_limit: int,
    income_limit: int,
    income_freq: str,
    timeout_sec: int,
) -> Dict[str, Any]:
    started_at = dt.datetime.now(dt.timezone.utc).isoformat()
    symbol_reports: List[Dict[str, Any]] = []

    counts = {"pass": 0, "warn": 0, "fail": 0, "skip": 0}

    for symbol in symbols:
        swift_snapshot = fetch_swift_snapshot(
            swift_bin=swift_bin,
            package_path=package_path,
            symbol=symbol,
            period=period,
            interval=interval,
            history_limit=history_limit,
            earnings_limit=earnings_limit,
            income_limit=income_limit,
            income_freq=income_freq,
            timeout_sec=timeout_sec,
        )
        python_snapshot = fetch_python_snapshot(
            symbol=symbol,
            period=period,
            interval=interval,
            history_limit=history_limit,
            earnings_limit=earnings_limit,
            income_limit=income_limit,
            income_freq=income_freq,
        )

        comparisons = compare_symbol(swift_snapshot, python_snapshot)
        symbol_status = worst_status([c.status for c in comparisons.values()])

        symbol_report = {
            "symbol": symbol,
            "status": symbol_status,
            "swift_ok": bool(swift_snapshot.get("ok", False)),
            "swift_errors": swift_snapshot.get("errors", []),
            "comparisons": {
                name: {
                    "status": result.status,
                    "summary": result.summary,
                    "metrics": result.metrics,
                    "issues": result.issues,
                }
                for name, result in comparisons.items()
            },
        }
        symbol_reports.append(symbol_report)

        counts[symbol_status] += 1

    total = len(symbol_reports)
    scored_total = max(0, total - counts["skip"])
    if scored_total == 0:
        score = 100.0
    else:
        score = max(0.0, ((counts["pass"] + 0.5 * counts["warn"]) / scored_total) * 100.0)

    return {
        "generated_at": started_at,
        "config": {
            "symbols": symbols,
            "period": period,
            "interval": interval,
            "history_limit": history_limit,
            "earnings_limit": earnings_limit,
            "income_limit": income_limit,
            "income_freq": income_freq,
            "package_path": str(package_path),
        },
        "summary": {
            "total": total,
            "pass": counts["pass"],
            "warn": counts["warn"],
            "fail": counts["fail"],
            "skip": counts["skip"],
            "score": score,
        },
        "symbols": symbol_reports,
    }


def fetch_swift_snapshot(
    *,
    swift_bin: str,
    package_path: Path,
    symbol: str,
    period: str,
    interval: str,
    history_limit: int,
    earnings_limit: int,
    income_limit: int,
    income_freq: str,
    timeout_sec: int,
) -> Dict[str, Any]:
    cmd = [
        swift_bin,
        "run",
        "--package-path",
        str(package_path),
        "YFParityCLI",
        "snapshot",
        "--symbol",
        symbol,
        "--period",
        period,
        "--interval",
        interval,
        "--history-limit",
        str(history_limit),
        "--earnings-limit",
        str(earnings_limit),
        "--income-limit",
        str(income_limit),
        "--freq",
        income_freq,
    ]

    try:
        proc = subprocess.run(
            cmd,
            text=True,
            capture_output=True,
            timeout=timeout_sec,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {"ok": False, "errors": [{"operation": "snapshot", "error": "swift_snapshot_timeout"}]}

    payload = parse_json_from_output(proc.stdout)
    if payload is None:
        return {
            "ok": False,
            "errors": [
                {
                    "operation": "snapshot",
                    "error": "swift_snapshot_invalid_json",
                    "stdout": proc.stdout.strip()[-400:],
                    "stderr": proc.stderr.strip()[-400:],
                }
            ],
        }

    if proc.returncode != 0 and payload.get("ok", False):
        payload["ok"] = False
        payload.setdefault("errors", []).append(
            {"operation": "snapshot", "error": f"swift_exit_{proc.returncode}"}
        )
    return payload


def parse_json_from_output(stdout: str) -> Optional[Dict[str, Any]]:
    text = stdout.strip()
    if not text:
        return None
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for line in reversed(lines):
        if line.startswith("{") and line.endswith("}"):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def fetch_python_snapshot(
    *,
    symbol: str,
    period: str,
    interval: str,
    history_limit: int,
    earnings_limit: int,
    income_limit: int,
    income_freq: str,
) -> Dict[str, Any]:
    ticker = yf.Ticker(symbol)
    errors: List[Dict[str, str]] = []

    quote: Dict[str, Any]
    history: Dict[str, Any]
    earnings: Dict[str, Any]
    income: Dict[str, Any]

    try:
        quote = python_quote(symbol, ticker)
    except Exception as exc:  # noqa: BLE001
        quote = {}
        errors.append({"operation": "quote", "error": str(exc)})

    try:
        history = python_history(ticker, period=period, interval=interval, limit=history_limit)
    except Exception as exc:  # noqa: BLE001
        history = {"period": period, "interval": interval, "barCount": 0, "bars": []}
        errors.append({"operation": "history", "error": str(exc)})

    try:
        earnings = python_earnings_dates(ticker, limit=earnings_limit)
    except Exception as exc:  # noqa: BLE001
        earnings = {"rowCount": 0, "rows": []}
        errors.append({"operation": "earnings-dates", "error": str(exc)})

    try:
        income = python_income_stmt(ticker, frequency=income_freq, limit=income_limit)
    except Exception as exc:  # noqa: BLE001
        income = {"frequency": income_freq, "rowCount": 0, "rows": []}
        errors.append({"operation": "income-stmt", "error": str(exc)})

    return {
        "ok": len(errors) == 0,
        "symbol": symbol,
        "quote": quote,
        "history": history,
        "earnings_dates": earnings,
        "income_stmt": income,
        "errors": errors,
    }


def python_quote(symbol: str, ticker: yf.Ticker) -> Dict[str, Any]:
    info = ticker.info or {}

    name = non_empty(info.get("longName"), info.get("shortName"))
    data = {
        "symbol": symbol,
        "name": name,
        "currency": info.get("currency"),
        "exchange": info.get("exchange"),
        "quoteType": info.get("quoteType"),
        "regularMarketPrice": to_float(info.get("regularMarketPrice")),
        "regularMarketChange": to_float(info.get("regularMarketChange")),
        "regularMarketChangePercent": to_float(info.get("regularMarketChangePercent")),
        "regularMarketVolume": to_int(info.get("regularMarketVolume")),
        "marketCap": to_float(info.get("marketCap")),
        "trailingPE": to_float(info.get("trailingPE")),
        "forwardPE": to_float(info.get("forwardPE")),
    }
    return data


def python_history(ticker: yf.Ticker, *, period: str, interval: str, limit: int) -> Dict[str, Any]:
    frame = ticker.history(
        period=period,
        interval=interval,
        prepost=False,
        actions=True,
        auto_adjust=True,
        back_adjust=False,
        repair=False,
        rounding=False,
    )
    if frame is None or frame.empty:
        return {"period": period, "interval": interval, "barCount": 0, "bars": []}

    frame = frame.tail(limit)
    intraday = ("m" in interval.lower()) or ("h" in interval.lower())
    rows: List[Dict[str, Any]] = []
    for index, row in frame.iterrows():
        rows.append(
            {
                "date": normalize_index_date(index, include_time=intraday),
                "open": to_float(row.get("Open")),
                "high": to_float(row.get("High")),
                "low": to_float(row.get("Low")),
                "close": to_float(row.get("Close")),
                "adjustedClose": to_float(row.get("Adj Close")),
                "volume": to_int(row.get("Volume")),
            }
        )
    return {"period": period, "interval": interval, "barCount": len(rows), "bars": rows}


def python_earnings_dates(ticker: yf.Ticker, *, limit: int) -> Dict[str, Any]:
    frame = ticker.get_earnings_dates(limit=max(limit * 4, 12))
    if frame is None or frame.empty:
        return {"rowCount": 0, "rows": []}

    frame = frame.reset_index()
    date_col = frame.columns[0]
    rows: List[Dict[str, Any]] = []
    seen = set()
    for _, row in frame.iterrows():
        date = normalize_timestamp_like(row.get(date_col))
        if not date or date in seen:
            continue
        seen.add(date)
        rows.append(
            {
                "date": date,
                "epsEstimate": to_float(row.get("EPS Estimate")),
                "epsActual": to_float(row.get("Reported EPS")),
                "surprisePercent": to_float(row.get("Surprise(%)")),
            }
        )
    rows.sort(key=lambda x: x.get("date") or "", reverse=True)
    rows = rows[:limit]
    return {"rowCount": len(rows), "rows": rows}


def python_income_stmt(ticker: yf.Ticker, *, frequency: str, limit: int) -> Dict[str, Any]:
    frame = ticker.income_stmt if frequency == "yearly" else ticker.quarterly_income_stmt
    if frame is None or frame.empty:
        return {"frequency": frequency, "rowCount": 0, "rows": []}

    index_map = {str(name).strip().lower(): name for name in frame.index}

    revenue_key = find_index_key(
        index_map,
        [
            "total revenue",
            "revenue",
            "operating revenue",
            "total operating revenue",
        ],
    )
    income_key = find_index_key(
        index_map,
        [
            "net income",
            "net income common stockholders",
            "net income continuing operations",
        ],
    )

    rows: List[Dict[str, Any]] = []
    for column in frame.columns:
        year = normalize_year_like(column)
        if not year:
            continue

        total_revenue = to_float(frame.loc[revenue_key, column]) if revenue_key is not None else None
        net_income = to_float(frame.loc[income_key, column]) if income_key is not None else None
        if total_revenue is None and net_income is None:
            continue
        rows.append({"year": year, "totalRevenue": total_revenue, "netIncome": net_income})

    rows.sort(key=lambda x: x.get("year") or "", reverse=True)
    rows = rows[:limit]
    return {"frequency": frequency, "rowCount": len(rows), "rows": rows}


def compare_symbol(swift_snapshot: Dict[str, Any], python_snapshot: Dict[str, Any]) -> Dict[str, CompareResult]:
    return {
        "quote": compare_quote(swift_snapshot.get("quote"), python_snapshot.get("quote")),
        "history": compare_history(swift_snapshot.get("history"), python_snapshot.get("history")),
        "earnings_dates": compare_earnings(
            swift_snapshot.get("earnings_dates"), python_snapshot.get("earnings_dates")
        ),
        "income_stmt": compare_income(swift_snapshot.get("income_stmt"), python_snapshot.get("income_stmt")),
    }


def compare_quote(swift: Any, py: Any) -> CompareResult:
    swift = swift if isinstance(swift, dict) else {}
    py = py if isinstance(py, dict) else {}

    issues: List[str] = []
    warns = 0
    fails = 0

    # Core symbol/currency/type checks
    for key in ("symbol", "currency", "quoteType"):
        s = normalize_text(swift.get(key))
        p = normalize_text(py.get(key))
        if s and p and s != p:
            fails += 1
            issues.append(f"{key}: swift={swift.get(key)} python={py.get(key)}")

    name_s = normalize_text(swift.get("name"))
    name_p = normalize_text(py.get("name"))
    if name_s and name_p and name_s != name_p:
        warns += 1
        issues.append(f"name differs: swift={swift.get('name')} python={py.get('name')}")

    numeric_fields = {
        "regularMarketPrice": 0.03,
        "regularMarketChangePercent": 0.30,
        "regularMarketVolume": 0.35,
        "marketCap": 0.15,
        "trailingPE": 0.25,
        "forwardPE": 0.25,
    }
    for field, tolerance in numeric_fields.items():
        s = to_float(swift.get(field))
        p = to_float(py.get(field))
        if s is None and p is None:
            continue
        if s is None or p is None:
            warns += 1
            issues.append(f"{field}: missing side swift={s} python={p}")
            continue
        if not nearly_equal(s, p, rel_tol=tolerance, abs_tol=1e-6):
            warns += 1
            issues.append(f"{field}: swift={s:.6g} python={p:.6g}")

    status = "pass"
    if fails > 0:
        status = "fail"
    elif warns > 0:
        status = "warn"

    return CompareResult(
        status=status,
        summary=f"{fails} fail-level, {warns} warn-level differences",
        metrics={"fail_diffs": fails, "warn_diffs": warns},
        issues=issues,
    )


def compare_history(swift: Any, py: Any) -> CompareResult:
    swift_rows = ((swift or {}).get("bars") or []) if isinstance(swift, dict) else []
    py_rows = ((py or {}).get("bars") or []) if isinstance(py, dict) else []
    swift_rows = [row for row in swift_rows if isinstance(row, dict)]
    py_rows = [row for row in py_rows if isinstance(row, dict)]

    if not swift_rows and not py_rows:
        return CompareResult("skip", "No history bars from either side", {"overlap": 0}, [])
    if not swift_rows or not py_rows:
        return CompareResult(
            "fail",
            "History only returned on one side",
            {"swift_count": len(swift_rows), "python_count": len(py_rows)},
            [f"swift_count={len(swift_rows)} python_count={len(py_rows)}"],
        )

    swift_map = {str(r.get("date")): r for r in swift_rows if r.get("date")}
    py_map = {str(r.get("date")): r for r in py_rows if r.get("date")}
    overlap_dates = sorted(set(swift_map.keys()) & set(py_map.keys()))
    if not overlap_dates:
        return CompareResult(
            "fail",
            "No overlapping history dates",
            {"swift_count": len(swift_rows), "python_count": len(py_rows), "overlap": 0},
            [],
        )

    close_diffs: List[float] = []
    for date in overlap_dates:
        s_close = to_float(swift_map[date].get("close"))
        p_close = to_float(py_map[date].get("close"))
        if s_close is None or p_close is None:
            continue
        denom = max(abs(p_close), 1e-9)
        close_diffs.append(abs(s_close - p_close) / denom)

    avg_diff = (sum(close_diffs) / len(close_diffs)) if close_diffs else 0.0
    count_delta = abs(len(swift_rows) - len(py_rows))

    if avg_diff <= 0.03 and count_delta <= 2:
        status = "pass"
    elif avg_diff <= 0.10:
        status = "warn"
    else:
        status = "fail"

    issues = []
    if count_delta > 2:
        issues.append(f"bar count delta={count_delta}")
    if avg_diff > 0.03:
        issues.append(f"avg close rel diff={avg_diff:.4f}")

    return CompareResult(
        status=status,
        summary=f"overlap={len(overlap_dates)} avg_close_rel_diff={avg_diff:.4f}",
        metrics={
            "swift_count": len(swift_rows),
            "python_count": len(py_rows),
            "overlap": len(overlap_dates),
            "avg_close_rel_diff": avg_diff,
        },
        issues=issues,
    )


def compare_earnings(swift: Any, py: Any) -> CompareResult:
    swift_rows = ((swift or {}).get("rows") or []) if isinstance(swift, dict) else []
    py_rows = ((py or {}).get("rows") or []) if isinstance(py, dict) else []
    swift_rows = [row for row in swift_rows if isinstance(row, dict)]
    py_rows = [row for row in py_rows if isinstance(row, dict)]

    if not swift_rows and not py_rows:
        return CompareResult("skip", "No earnings rows from either side", {"overlap": 0}, [])
    if not swift_rows or not py_rows:
        return CompareResult(
            "fail",
            "Earnings rows present on only one side",
            {"swift_count": len(swift_rows), "python_count": len(py_rows)},
            [],
        )

    swift_map = {str(row.get("date")): row for row in swift_rows if row.get("date")}
    py_map = {str(row.get("date")): row for row in py_rows if row.get("date")}
    overlap = sorted(set(swift_map.keys()) & set(py_map.keys()))
    if not overlap:
        return CompareResult("warn", "No overlapping earnings dates", {"overlap": 0}, [])

    diffs: List[float] = []
    for date in overlap:
        for field in ("epsEstimate", "epsActual"):
            s = to_float(swift_map[date].get(field))
            p = to_float(py_map[date].get(field))
            if s is None or p is None:
                continue
            denom = max(abs(p), 1e-9)
            diffs.append(abs(s - p) / denom)

    avg_diff = (sum(diffs) / len(diffs)) if diffs else 0.0
    if len(overlap) >= 2 and avg_diff <= 0.10:
        status = "pass"
    elif avg_diff <= 0.25:
        status = "warn"
    else:
        status = "fail"

    issues: List[str] = []
    if len(overlap) < 2:
        issues.append("low overlap (<2 dates)")
    if avg_diff > 0.10:
        issues.append(f"avg EPS rel diff={avg_diff:.4f}")

    return CompareResult(
        status=status,
        summary=f"overlap={len(overlap)} avg_eps_rel_diff={avg_diff:.4f}",
        metrics={"swift_count": len(swift_rows), "python_count": len(py_rows), "overlap": len(overlap), "avg_eps_rel_diff": avg_diff},
        issues=issues,
    )


def compare_income(swift: Any, py: Any) -> CompareResult:
    swift_rows = ((swift or {}).get("rows") or []) if isinstance(swift, dict) else []
    py_rows = ((py or {}).get("rows") or []) if isinstance(py, dict) else []
    swift_rows = [row for row in swift_rows if isinstance(row, dict)]
    py_rows = [row for row in py_rows if isinstance(row, dict)]

    if not swift_rows and not py_rows:
        return CompareResult("skip", "No income rows from either side", {"overlap": 0}, [])
    if not swift_rows or not py_rows:
        return CompareResult(
            "fail",
            "Income rows present on only one side",
            {"swift_count": len(swift_rows), "python_count": len(py_rows)},
            [],
        )

    swift_map = {str(row.get("year")): row for row in swift_rows if row.get("year")}
    py_map = {str(row.get("year")): row for row in py_rows if row.get("year")}
    overlap = sorted(set(swift_map.keys()) & set(py_map.keys()), reverse=True)
    if not overlap:
        return CompareResult("warn", "No overlapping income years", {"overlap": 0}, [])

    diffs: List[float] = []
    for year in overlap:
        for field in ("totalRevenue", "netIncome"):
            s = to_float(swift_map[year].get(field))
            p = to_float(py_map[year].get(field))
            if s is None or p is None:
                continue
            denom = max(abs(p), 1e-9)
            diffs.append(abs(s - p) / denom)

    avg_diff = (sum(diffs) / len(diffs)) if diffs else 0.0
    if len(overlap) >= 2 and avg_diff <= 0.15:
        status = "pass"
    elif avg_diff <= 0.35:
        status = "warn"
    else:
        status = "fail"

    issues: List[str] = []
    if len(overlap) < 2:
        issues.append("low overlap (<2 years)")
    if avg_diff > 0.15:
        issues.append(f"avg statement rel diff={avg_diff:.4f}")

    return CompareResult(
        status=status,
        summary=f"overlap={len(overlap)} avg_stmt_rel_diff={avg_diff:.4f}",
        metrics={"swift_count": len(swift_rows), "python_count": len(py_rows), "overlap": len(overlap), "avg_stmt_rel_diff": avg_diff},
        issues=issues,
    )


def render_markdown(report: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("# YFinanceKit Parity Report")
    lines.append("")
    lines.append(f"- Generated: `{report.get('generated_at', '')}`")
    cfg = report.get("config", {})
    lines.append(
        f"- Config: period=`{cfg.get('period')}` interval=`{cfg.get('interval')}` "
        f"history_limit={cfg.get('history_limit')} earnings_limit={cfg.get('earnings_limit')} "
        f"income_limit={cfg.get('income_limit')} income_freq=`{cfg.get('income_freq')}`"
    )
    summary = report.get("summary", {})
    lines.append(
        f"- Summary: pass={summary.get('pass', 0)} warn={summary.get('warn', 0)} "
        f"fail={summary.get('fail', 0)} skip={summary.get('skip', 0)} score={summary.get('score', 0):.1f}"
    )
    lines.append("")
    lines.append("## Symbol Status")
    lines.append("")
    lines.append("| Symbol | Status | Quote | History | Earnings | Income |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for symbol_report in report.get("symbols", []):
        comps = symbol_report.get("comparisons", {})
        lines.append(
            "| {symbol} | {status} | {quote} | {history} | {earnings} | {income} |".format(
                symbol=symbol_report.get("symbol", ""),
                status=symbol_report.get("status", ""),
                quote=((comps.get("quote") or {}).get("status", "")),
                history=((comps.get("history") or {}).get("status", "")),
                earnings=((comps.get("earnings_dates") or {}).get("status", "")),
                income=((comps.get("income_stmt") or {}).get("status", "")),
            )
        )

    lines.append("")
    lines.append("## Details")
    lines.append("")
    for symbol_report in report.get("symbols", []):
        lines.append(f"### {symbol_report.get('symbol', '')} (`{symbol_report.get('status', '')}`)")
        swift_errors = symbol_report.get("swift_errors") or []
        if swift_errors:
            lines.append("- Swift errors:")
            for err in swift_errors:
                lines.append(f"  - `{err}`")
        comps = symbol_report.get("comparisons", {})
        for key in ("quote", "history", "earnings_dates", "income_stmt"):
            comp = comps.get(key, {})
            lines.append(f"- `{key}`: **{comp.get('status', 'unknown')}** - {comp.get('summary', '')}")
            issues = comp.get("issues") or []
            for issue in issues:
                lines.append(f"  - {issue}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(num) or math.isinf(num):
        return None
    return num


def to_int(value: Any) -> Optional[int]:
    num = to_float(value)
    if num is None:
        return None
    return int(num)


def non_empty(*values: Any) -> Optional[str]:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def nearly_equal(a: float, b: float, rel_tol: float, abs_tol: float = 1e-9) -> bool:
    if math.isclose(a, b, rel_tol=rel_tol, abs_tol=abs_tol):
        return True
    return False


def normalize_index_date(value: Any, include_time: bool) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        if include_time:
            ts = value.tz_convert("UTC") if value.tzinfo else value.tz_localize("UTC")
            return ts.strftime("%Y-%m-%dT%H:%M:%SZ")
        return value.strftime("%Y-%m-%d")
    if isinstance(value, dt.datetime):
        if include_time:
            if value.tzinfo is None:
                value = value.replace(tzinfo=dt.timezone.utc)
            value = value.astimezone(dt.timezone.utc)
            return value.strftime("%Y-%m-%dT%H:%M:%SZ")
        return value.strftime("%Y-%m-%d")
    if isinstance(value, dt.date):
        return value.strftime("%Y-%m-%d")
    return normalize_timestamp_like(value)


def normalize_timestamp_like(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, dt.datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, dt.date):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, (int, float)):
        seconds = float(value)
        if abs(seconds) > 9_999_999_999:
            seconds /= 1000.0
        if abs(seconds) > 1_000_000:
            return dt.datetime.fromtimestamp(seconds, tz=dt.timezone.utc).strftime("%Y-%m-%d")
        return None

    text = str(value).strip()
    if not text:
        return None
    if len(text) >= 10 and text[4:5] == "-" and text[7:8] == "-":
        return text[:10]
    try:
        parsed = pd.to_datetime(text, errors="coerce")
        if pd.isna(parsed):
            return None
        if isinstance(parsed, pd.Timestamp):
            return parsed.strftime("%Y-%m-%d")
    except Exception:  # noqa: BLE001
        return None
    return None


def normalize_year_like(value: Any) -> Optional[str]:
    date = normalize_timestamp_like(value)
    if date and len(date) >= 4:
        return date[:4]
    if value is None:
        return None
    text = str(value).strip()
    if len(text) >= 4 and text[:4].isdigit():
        year = int(text[:4])
        if 1900 <= year <= 2100:
            return str(year)
    return None


def find_index_key(index_map: Dict[str, Any], candidates: Iterable[str]) -> Any:
    for candidate in candidates:
        key = index_map.get(candidate.lower())
        if key is not None:
            return key
    return None


def worst_status(statuses: Iterable[str]) -> str:
    worst = "pass"
    for status in statuses:
        if STATUS_ORDER.get(status, 99) > STATUS_ORDER.get(worst, 0):
            worst = status
    return worst


if __name__ == "__main__":
    raise SystemExit(main())
