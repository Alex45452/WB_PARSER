from __future__ import annotations

from typing import Any, List, Tuple

import psycopg
from psycopg.rows import tuple_row

from config import DB_DSN, SOURCE_TABLE, RESULT_TABLE, PARSE_VERSION


def connect():
    conn = psycopg.connect(DB_DSN, autocommit=False)
    conn.row_factory = tuple_row
    return conn


def stream_cards(conn, only_unprocessed: bool, reprocess_all: bool, batch_size: int):
    where = []
    params: List[Any] = []
    if not reprocess_all:
        if only_unprocessed:
            where.append("(parse_status IS NULL OR parse_status = 0)")
        else:
            where.append("(parse_status = 0)")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    sql = f"""
      SELECT nm_id, title, description
      FROM {SOURCE_TABLE}
      {where_sql}
      ORDER BY nm_id
    """

    cur = conn.cursor(name="wb_stream")
    cur.itersize = batch_size
    cur.execute(sql, params)
    return cur


def upsert_results(conn, rows: List[Tuple[Any, ...]]):
    """
    rows: (nm_id, ver, decision, category, target, reject_reason, title_used, matched_in)
    """
    sql = f"""
      INSERT INTO {RESULT_TABLE}
        (nm_id, ver, decision, category, target, reject_reason, title_used, matched_in)
      VALUES
        (%s,%s,%s,%s,%s,%s,%s,%s)
      ON CONFLICT (nm_id) DO UPDATE SET
        ver = EXCLUDED.ver,
        decision = EXCLUDED.decision,
        category = EXCLUDED.category,
        target = EXCLUDED.target,
        reject_reason = EXCLUDED.reject_reason,
        title_used = EXCLUDED.title_used,
        matched_in = EXCLUDED.matched_in,
        created_at = now()
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)


def mark_processed(conn, nm_ids: List[int]):
    sql = f"""
      UPDATE {SOURCE_TABLE}
      SET parse_status = 1,
          parse_version = %s,
          parsed_at = now()
      WHERE nm_id = ANY(%s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (PARSE_VERSION, nm_ids))


def mark_error(conn, nm_ids: List[int]):
    sql = f"""
      UPDATE {SOURCE_TABLE}
      SET parse_status = 2,
          parse_version = %s,
          parsed_at = now()
      WHERE nm_id = ANY(%s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (PARSE_VERSION, nm_ids))