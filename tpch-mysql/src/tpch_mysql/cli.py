from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import tpchgen

from .mysql import (
    MySqlConfig,
    connect_mysql,
    load_data_infile,
    run_sql_script,
    try_set_session_max_execution_time,
)
from .queries import QUERIES
from .schema import SCHEMA_SQL


TABLES = ("nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem")
TABLE_TO_FILENAME = {
    "nation": "nation.csv",
    "region": "region.csv",
    "part": "part.csv",
    "supplier": "supplier.csv",
    "partsupp": "partsupp.csv",
    "customer": "customer.csv",
    "orders": "orders.csv",
    "lineitem": "lineitem.csv",
}


def _add_mysql_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--host", required=True)
    p.add_argument("--port", required=True, type=int)
    p.add_argument("--user", required=True)
    p.add_argument("--password", required=True)
    p.add_argument("--database", required=True)


def _parse_tables(value: str) -> list[str]:
    items = [v.strip().lower() for v in value.split(",") if v.strip()]
    unknown = [t for t in items if t not in TABLES]
    if unknown:
        raise argparse.ArgumentTypeError(f"Unknown tables: {', '.join(unknown)}")
    return items


def cmd_schema(args: argparse.Namespace) -> int:
    cfg = MySqlConfig(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
    )
    conn = connect_mysql(cfg)
    try:
        run_sql_script(conn, SCHEMA_SQL)
    finally:
        conn.close()
    return 0


def cmd_gen(args: argparse.Namespace) -> int:
    output_dir = Path(args.data_dir)
    tpchgen.generate(
        scale_factor=args.scale_factor,
        output_dir=output_dir,
        tables=None,
        format="csv",
        delimiter="\t",
        parts=None,
        part=None,
        num_threads=args.threads,
    )
    return 0


def cmd_load(args: argparse.Namespace) -> int:
    cfg = MySqlConfig(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
    )
    data_dir = Path(args.data_dir)
    tables = args.tables or list(TABLES)
    conn = connect_mysql(cfg)
    try:
        if args.truncate:
            with conn.cursor() as cur:
                for table in tables:
                    cur.execute(f"TRUNCATE TABLE `{table}`")
        for table in tables:
            file_path = data_dir / TABLE_TO_FILENAME[table]
            load_data_infile(
                conn,
                table=table,
                file_path=file_path,
                field_terminated_by="\t",
                line_terminated_by="\n",
                ignore_header_lines=args.ignore_header_lines,
            )
    finally:
        conn.close()
    return 0


def _parse_query_ids(values: list[str]) -> list[int]:
    out: list[int] = []
    for v in values:
        for token in v.split(","):
            token = token.strip()
            if not token:
                continue
            try:
                qid = int(token)
            except ValueError as e:
                raise argparse.ArgumentTypeError(f"Invalid query id: {token}") from e
            out.append(qid)
    unknown = [qid for qid in out if qid not in QUERIES]
    if unknown:
        raise argparse.ArgumentTypeError(f"Unknown query ids: {', '.join(map(str, unknown))}")
    return out


def _is_select_statement(sql: str) -> bool:
    s = sql.lstrip().lower()
    return s.startswith("select") or s.startswith("with")


def _apply_max_execution_time_hint(sql: str, timeout_ms: int | None) -> str:
    if timeout_ms is None:
        return sql
    stripped = sql.lstrip()
    if not stripped[:6].lower() == "select":
        return sql
    indent = sql[: len(sql) - len(stripped)]
    return f"{indent}select /*+ MAX_EXECUTION_TIME({timeout_ms}) */{stripped[6:]}"


def cmd_run(args: argparse.Namespace) -> int:
    cfg = MySqlConfig(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
    )
    query_ids = list(range(1, 23)) if args.all else _parse_query_ids(args.query)
    timeout_ms = None if args.timeout_seconds <= 0 else int(args.timeout_seconds * 1000)

    results: list[dict[str, object]] = []
    conn = connect_mysql(cfg)
    try:
        for qid in query_ids:
            q = QUERIES[qid]

            result: dict[str, object] = {
                "query_id": q.query_id,
                "title": q.title,
                "ok": False,
            }

            try:
                try:
                    try_set_session_max_execution_time(conn, milliseconds=timeout_ms)
                except Exception:
                    pass
                start = time.perf_counter()
                rows = 0
                with conn.cursor() as cur:
                    for stmt in q.statements:
                        stmt = _apply_max_execution_time_hint(stmt, timeout_ms)
                        cur.execute(stmt)
                        if _is_select_statement(stmt):
                            rows += len(cur.fetchall())
                elapsed = time.perf_counter() - start
                result["ok"] = True
                result["seconds"] = elapsed
                result["rows"] = rows
            except Exception as e:  # noqa: BLE001 - surface DB errors to caller
                elapsed = time.perf_counter() - start
                result["seconds"] = elapsed
                result["error"] = str(e)

            results.append(result)
    finally:
        conn.close()

    if args.output:
        Path(args.output).write_text(json.dumps(results, ensure_ascii=False, indent=2) + "\n")
    else:
        print(json.dumps(results, ensure_ascii=False, indent=2))

    return 0


def cmd_all(args: argparse.Namespace) -> int:
    rc = cmd_schema(args)
    if rc != 0:
        return rc
    rc = cmd_gen(args)
    if rc != 0:
        return rc
    return cmd_load(args)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="tpch-mysql")
    sub = p.add_subparsers(dest="command", required=True)

    p_schema = sub.add_parser("schema", help="Create TPC-H tables in MySQL")
    _add_mysql_args(p_schema)
    p_schema.set_defaults(func=cmd_schema)

    p_gen = sub.add_parser("gen", help="Generate TPC-H data files with tpchgen-py")
    p_gen.add_argument("--data-dir", required=True)
    p_gen.add_argument("--scale-factor", required=True, type=float)
    p_gen.add_argument("--threads", type=int)
    p_gen.set_defaults(func=cmd_gen)

    p_load = sub.add_parser("load", help="Load generated files into MySQL using LOAD DATA INFILE")
    _add_mysql_args(p_load)
    p_load.add_argument("--data-dir", required=True)
    p_load.add_argument("--tables", type=_parse_tables, help="Comma-separated table list")
    p_load.add_argument("--ignore-header-lines", type=int, default=1)
    p_load.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate target tables before loading (disabled by default)",
    )
    p_load.set_defaults(func=cmd_load)

    p_all = sub.add_parser("all", help="Run schema + gen + load")
    _add_mysql_args(p_all)
    p_all.add_argument("--data-dir", required=True)
    p_all.add_argument("--scale-factor", required=True, type=float)
    p_all.add_argument("--threads", type=int)
    p_all.add_argument("--tables", type=_parse_tables, help="Comma-separated table list")
    p_all.add_argument("--ignore-header-lines", type=int, default=1)
    p_all.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate target tables before loading (disabled by default)",
    )
    p_all.set_defaults(func=cmd_all)

    p_run = sub.add_parser("run", help="Run TPC-H queries and record timings")
    _add_mysql_args(p_run)
    group = p_run.add_mutually_exclusive_group(required=True)
    group.add_argument("--all", action="store_true", help="Run Q1..Q22")
    group.add_argument(
        "--query",
        action="append",
        default=[],
        help="Query ids (repeatable or comma-separated), e.g. --query 1 --query 6,19",
    )
    p_run.add_argument(
        "--timeout-seconds",
        type=int,
        default=0,
        help="Per-query server-side timeout via MAX_EXECUTION_TIME (0 disables)",
    )
    p_run.add_argument("--output", help="Write JSON results to this path (default: stdout)")
    p_run.set_defaults(func=cmd_run)

    return p


def main(argv: list[str] | None = None) -> None:
    p = build_parser()
    args = p.parse_args(argv)
    raise SystemExit(args.func(args))
