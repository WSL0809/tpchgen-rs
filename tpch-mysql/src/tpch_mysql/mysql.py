from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pymysql


@dataclass(frozen=True)
class MySqlConfig:
    host: str
    port: int
    user: str
    password: str
    database: str


def connect_mysql(cfg: MySqlConfig) -> pymysql.connections.Connection:
    return pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        autocommit=True,
    )


def run_sql_script(conn: pymysql.connections.Connection, sql: str) -> None:
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    with conn.cursor() as cur:
        for stmt in statements:
            cur.execute(stmt)


def run_sql_file(conn: pymysql.connections.Connection, path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    run_sql_script(conn, sql)


def escape_mysql_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def load_data_infile(
    conn: pymysql.connections.Connection,
    *,
    table: str,
    file_path: Path,
    field_terminated_by: str = "\t",
    line_terminated_by: str = "\n",
    ignore_header_lines: int = 1,
) -> None:
    infile = escape_mysql_string(str(file_path))
    fields = escape_mysql_string(field_terminated_by)
    lines = escape_mysql_string(line_terminated_by)

    sql = (
        f"LOAD DATA INFILE '{infile}' INTO TABLE {table} "
    )
    with conn.cursor() as cur:
        cur.execute(sql)


def try_set_session_max_execution_time(
    conn: pymysql.connections.Connection, *, milliseconds: int | None
) -> bool:
    if milliseconds is None:
        return True
    with conn.cursor() as cur:
        cur.execute(f"SET SESSION max_execution_time={int(milliseconds)}")
    return True
