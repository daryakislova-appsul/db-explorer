#!/usr/bin/env python3
"""Generate schema.json for the DB Explorer dashboard from ClickHouse system tables."""

import sys, os, json

if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

CH_HOST = os.environ.get("CH_HOST", "w9g34itw1t.us-east-1.aws.clickhouse.cloud")
CH_PORT = int(os.environ.get("CH_PORT", "8443"))
CH_USER = os.environ.get("CH_USER", "default")
CH_PASSWORD = os.environ.get("CH_PASSWORD", "")

import clickhouse_connect

client = clickhouse_connect.get_client(
    host=CH_HOST, port=CH_PORT,
    username=CH_USER, password=CH_PASSWORD,
    secure=True, connect_timeout=30,
    settings={"max_execution_time": 300},
)

DATABASES = ["gold", "bronze", "silver"]

# ── Query schemas ─────────────────────────────────────────────────────────────
col_sql = """
SELECT database, table, name, type, comment, position
FROM system.columns
WHERE database IN ('gold', 'bronze', 'silver')
ORDER BY database, table, position
"""
col_result = client.query(col_sql)

stat_sql = """
SELECT database, name AS table, total_rows, total_bytes, engine
FROM system.tables
WHERE database IN ('gold', 'bronze', 'silver')
ORDER BY database, name
"""
stat_result = client.query(stat_sql)

stats = {}
for r in stat_result.result_rows:
    stats[(r[0], r[1])] = {"rows": r[2], "bytes": r[3], "engine": r[4]}

tables = {}
for r in col_result.result_rows:
    db, tbl, col_name, col_type, comment, pos = r
    key = f"{db}.{tbl}"
    if key not in tables:
        s = stats.get((db, tbl), {})
        tables[key] = {
            "database": db,
            "table": tbl,
            "full_name": key,
            "rows": s.get("rows", 0),
            "bytes": s.get("bytes", 0),
            "engine": s.get("engine", ""),
            "columns": [],
            "tags": [],
            "purpose": "",
            "time_col": "",
            "time_warn": "",
            "lineage": "",
            "lineage_upstream": [],
            "lineage_downstream": [],
            "revenue": "",
            "notes": [],
        }
    tables[key]["columns"].append({
        "name": col_name,
        "type": col_type,
        "comment": comment or "",
        "position": pos,
    })

# ── Load annotations ──────────────────────────────────────────────────────────
ann_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "annotations.json")
if os.path.exists(ann_path):
    with open(ann_path, "r", encoding="utf-8") as f:
        ann_data = json.load(f)
    for key, ann in ann_data.items():
        if key in tables:
            for field in ["tags", "purpose", "time_col", "time_warn", "lineage",
                          "lineage_upstream", "lineage_downstream", "revenue", "notes"]:
                if field in ann:
                    tables[key][field] = ann[field]

# ── Build output ──────────────────────────────────────────────────────────────
db_summary = {}
for db in DATABASES:
    db_tables = [t for t in tables.values() if t["database"] == db]
    db_summary[db] = {
        "table_count": len(db_tables),
        "total_rows": sum(t["rows"] or 0 for t in db_tables),
        "total_bytes": sum(t["bytes"] or 0 for t in db_tables),
        "column_count": sum(len(t["columns"]) for t in db_tables),
    }

from datetime import datetime, timezone
output = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "databases": DATABASES,
    "summary": db_summary,
    "total_tables": len(tables),
    "total_columns": sum(len(t["columns"]) for t in tables.values()),
    "tables": tables,
}

out_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "schema.json")
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(output, f, indent=2, default=str)

print(f"Done! {len(tables)} tables, {output['total_columns']} columns -> {out_path}")
