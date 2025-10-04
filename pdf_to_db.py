# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-

# """
# Load CSVs (well_header.csv & well_stimulation.csv) into MySQL database.

# Usage:
#   python csv_to_db.py \
#     --header well_header.csv \
#     --stim well_stimulation.csv
# """

# import os, csv
# import mysql.connector
# from dataclasses import dataclass
# from typing import Optional
# from dotenv import load_dotenv

# load_dotenv()

# # -------------------- DB Config --------------------
# DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
# DB_PORT = int(os.getenv("DB_PORT", 3306))
# DB_USER = os.getenv("DB_USER", "phpmyadmin")
# DB_PASS = os.getenv("DB_PASS", "root")
# DB_NAME = os.getenv("DB_NAME", "oilwell_pdf_extraction")

# # -------------------- DB Schema --------------------
# def init_db():
#     """Create database and tables if needed."""
#     conn = mysql.connector.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS)
#     cur = conn.cursor()
#     cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
#     cur.execute(f"USE {DB_NAME}")

#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS well_header (
#             pdf_name            VARCHAR(255) PRIMARY KEY,
#             operator            VARCHAR(255),
#             well_name           VARCHAR(255),
#             api                 VARCHAR(32),
#             enseco_job          VARCHAR(64),
#             job_type            VARCHAR(128),
#             county_state        VARCHAR(256),
#             shl                 TEXT,
#             latitude            DECIMAL(12,9),
#             longitude           DECIMAL(12,9),
#             datum               VARCHAR(128)
#         )
#     """)

#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS well_stimulation (
#             pdf_name                    VARCHAR(255) PRIMARY KEY,
#             date_simulated              VARCHAR(32),
#             stimulated_formation        VARCHAR(128),
#             type_treatment              VARCHAR(128),
#             acid_pct                    VARCHAR(32),
#             lbs_proppant                VARCHAR(32),
#             top_ft                      VARCHAR(32),
#             bottom_ft                   VARCHAR(32),
#             stimulation_stages          VARCHAR(32),
#             volume                      VARCHAR(32),
#             volume_units                VARCHAR(32),
#             max_pressure_psi            VARCHAR(32),
#             max_treatment_rate_bbls_min VARCHAR(32),
#             details                     TEXT,
#             updated_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
#             CONSTRAINT fk_stim_pdf
#                 FOREIGN KEY (pdf_name) REFERENCES well_header(pdf_name)
#                 ON DELETE CASCADE ON UPDATE CASCADE
#         )
#     """)

#     conn.commit()
#     cur.close()
#     conn.close()

# # -------------------- Load CSV → DB --------------------
# def upsert_header(conn, row: dict):
#     sql = """
#     INSERT INTO well_header
#       (pdf_name, operator, well_name, api, enseco_job, job_type,
#        county_state, shl, latitude, longitude, datum)
#     VALUES
#       (%(pdf_name)s, %(operator)s, %(well_name)s, %(api)s, %(enseco_job)s, %(job_type)s,
#        %(county_state)s, %(shl)s, %(latitude)s, %(longitude)s, %(datum)s)
#     ON DUPLICATE KEY UPDATE
#       operator=VALUES(operator),
#       well_name=VALUES(well_name),
#       api=VALUES(api),
#       enseco_job=VALUES(enseco_job),
#       job_type=VALUES(job_type),
#       county_state=VALUES(county_state),
#       shl=VALUES(shl),
#       latitude=VALUES(latitude),
#       longitude=VALUES(longitude),
#       datum=VALUES(datum);
#     """
#     cur = conn.cursor()
#     cur.execute(sql, row)
#     cur.close()

# def upsert_stimulation(conn, row: dict):
#     sql = """
#     INSERT INTO well_stimulation
#       (pdf_name, date_simulated, stimulated_formation, type_treatment, acid_pct,
#        lbs_proppant, top_ft, bottom_ft, stimulation_stages,
#        volume, volume_units, max_pressure_psi, max_treatment_rate_bbls_min, details)
#     VALUES
#       (%(pdf_name)s, %(date_simulated)s, %(stimulated_formation)s, %(type_treatment)s, %(acid_pct)s,
#        %(lbs_proppant)s, %(top_ft)s, %(bottom_ft)s, %(stimulation_stages)s,
#        %(volume)s, %(volume_units)s, %(max_pressure_psi)s, %(max_treatment_rate_bbls_min)s, %(details)s)
#     ON DUPLICATE KEY UPDATE
#       date_simulated=VALUES(date_simulated),
#       stimulated_formation=VALUES(stimulated_formation),
#       type_treatment=VALUES(type_treatment),
#       acid_pct=VALUES(acid_pct),
#       lbs_proppant=VALUES(lbs_proppant),
#       top_ft=VALUES(top_ft),
#       bottom_ft=VALUES(bottom_ft),
#       stimulation_stages=VALUES(stimulation_stages),
#       volume=VALUES(volume),
#       volume_units=VALUES(volume_units),
#       max_pressure_psi=VALUES(max_pressure_psi),
#       max_treatment_rate_bbls_min=VALUES(max_treatment_rate_bbls_min),
#       details=VALUES(details);
#     """
#     cur = conn.cursor()
#     cur.execute(sql, row)
#     cur.close()

# def load_csvs_to_db(header_csv: str, stim_csv: str):
#     init_db()
#     conn = mysql.connector.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, database=DB_NAME)

#     try:
#         # Load well_header.csv
#         with open(header_csv, newline="", encoding="utf-8") as f:
#             reader = csv.DictReader(f)
#             for row in reader:
#                 row["latitude"] = float(row["latitude"]) if row.get("latitude") else None
#                 row["longitude"] = float(row["longitude"]) if row.get("longitude") else None
#                 upsert_header(conn, row)

#         # Load well_stimulation.csv
#         with open(stim_csv, newline="", encoding="utf-8") as f:
#             reader = csv.DictReader(f)
#             for row in reader:
#                 upsert_stimulation(conn, row)

#         conn.commit()
#         print("[OK] Data imported successfully.")
#     finally:
#         conn.close()

# if __name__ == "__main__":
#     import argparse
#     parser = argparse.ArgumentParser("Load CSVs into MySQL")
#     parser.add_argument("--header", required=True, help="well_header.csv path")
#     parser.add_argument("--stim", required=True, help="well_stimulation.csv path")
#     args = parser.parse_args()

#     load_csvs_to_db(args.header, args.stim)


#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Load CSVs (well_header.csv & well_stimulation.csv) into MySQL database (idempotent upsert).

Usage:
  python csv_to_db.py \
    --header /path/to/well_header.csv \
    --stim   /path/to/well_stimulation.csv

Env (via .env or real env):
  DB_HOST=127.0.0.1
  DB_PORT=3306
  DB_USER=phpmyadmin
  DB_PASS=root
  DB_NAME=oilwell_pdf_extraction
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Robust CSV → MySQL loader with diagnostics.

Usage:
  python csv_to_db.py --header well_header.csv --stim well_stimulation.csv [--dry-run] [--limit N] [--verbose]

Env (.env or env):
  DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME
"""

import os, csv, argparse, sys
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, Tuple, Optional, List

from dotenv import load_dotenv
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER", "phpmyadmin")
DB_PASS = os.getenv("DB_PASS", "root")
DB_NAME = os.getenv("DB_NAME", "oilwell_pdf_extraction")

def _ensure_mysql():
    import mysql.connector  # noqa: F401
    return True

def find_col_name(header_keys: List[str], kind: str) -> Optional[str]:
    keys_norm = [(k, (k or "").strip()) for k in header_keys]
    lower_map = {k: v.lower() for k, v in keys_norm}

    preferred = "latitude" if kind == "lat" else "longitude"
    for k, low in lower_map.items():
        if low == preferred:
            return k

    hints = LAT_KEY_HINTS if kind == "lat" else LON_KEY_HINTS
    for k, low in lower_map.items():
        if any(h in low for h in hints):
            return k
    return None

def to_decimal(val) -> Optional[Decimal]:
    if val is None:
        return None
    s = str(val).strip()
    if s == "" or s.lower() in {"na", "n/a", "null", "none"}:
        return None
    s = (s.replace("−", "-").replace("–", "-").replace("—", "-"))
    s = s.replace(",", "").strip('"').strip("'")

    if "°" in s or "'" in s or '"' in s:
        import re
        m = re.match(r'^\s*([+-]?\d+)(?:[°\s]+(\d+))?(?:[\'\s]+([\d.]+))?"?\s*$', s)
        if m:
            deg = float(m.group(1))
            minutes = float(m.group(2)) if m.group(2) else 0.0
            seconds = float(m.group(3)) if m.group(3) else 0.0
            sign = -1 if deg < 0 else 1
            valf = abs(deg) + minutes/60.0 + seconds/3600.0
            valf *= sign
            try:
                return Decimal(str(valf))
            except InvalidOperation:
                return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None

def open_and_sniff(path: str):
    try:
        f = open(path, newline="", encoding="utf-8-sig")
        sample = f.read(4096)
        f.seek(0)
        dialect = csv.Sniffer().sniff(sample)
        reader = csv.DictReader(f, dialect=dialect)
        return f, reader
    except Exception:
        f = open(path, newline="", encoding="latin-1")
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
        except Exception:
            dialect = csv.excel
        reader = csv.DictReader(f, dialect=dialect)
        return f, reader

def init_db(conn):
    cur = conn.cursor()
    cur.execute(
        f"CREATE DATABASE IF NOT EXISTS {DB_NAME} "
        f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    )
    cur.execute(f"USE {DB_NAME}")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS well_header (
            pdf_name   VARCHAR(255) PRIMARY KEY,
            operator   VARCHAR(255),
            well_name  VARCHAR(255),
            api        VARCHAR(32),
            enseco_job VARCHAR(64),
            job_type   VARCHAR(128),
            county_state VARCHAR(256),
            shl        TEXT,
            latitude   DECIMAL(12,9),
            longitude  DECIMAL(12,9),
            datum      VARCHAR(128)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS well_stimulation (
            pdf_name VARCHAR(255) PRIMARY KEY,
            date_simulated VARCHAR(32),
            stimulated_formation VARCHAR(128),
            type_treatment VARCHAR(128),
            acid_pct VARCHAR(32),
            lbs_proppant VARCHAR(32),
            top_ft VARCHAR(32),
            bottom_ft VARCHAR(32),
            stimulation_stages VARCHAR(32),
            volume VARCHAR(32),
            volume_units VARCHAR(32),
            max_pressure_psi VARCHAR(32),
            max_treatment_rate_bbls_min VARCHAR(32),
            details TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            CONSTRAINT fk_stim_pdf FOREIGN KEY (pdf_name) REFERENCES well_header(pdf_name)
                ON DELETE CASCADE ON UPDATE CASCADE
        )
    """)
    cur.close()
    conn.commit()

def upsert_header(conn, row):
    sql = """
    INSERT INTO well_header
      (pdf_name, operator, well_name, api, enseco_job, job_type,
       county_state, shl, latitude, longitude, datum)
    VALUES
      (%(pdf_name)s, %(operator)s, %(well_name)s, %(api)s, %(enseco_job)s, %(job_type)s,
       %(county_state)s, %(shl)s, %(latitude)s, %(longitude)s, %(datum)s)
    ON DUPLICATE KEY UPDATE
      operator=VALUES(operator), well_name=VALUES(well_name), api=VALUES(api),
      enseco_job=VALUES(enseco_job), job_type=VALUES(job_type),
      county_state=VALUES(county_state), shl=VALUES(shl),
      latitude=VALUES(latitude), longitude=VALUES(longitude), datum=VALUES(datum);
    """
    cur = conn.cursor()
    cur.execute(sql, row); cur.close()

def upsert_stimulation(conn, row):
    sql = """
    INSERT INTO well_stimulation
      (pdf_name, date_simulated, stimulated_formation, type_treatment, acid_pct,
       lbs_proppant, top_ft, bottom_ft, stimulation_stages,
       volume, volume_units, max_pressure_psi, max_treatment_rate_bbls_min, details)
    VALUES
      (%(pdf_name)s, %(date_simulated)s, %(stimulated_formation)s, %(type_treatment)s, %(acid_pct)s,
       %(lbs_proppant)s, %(top_ft)s, %(bottom_ft)s, %(stimulation_stages)s,
       %(volume)s, %(volume_units)s, %(max_pressure_psi)s, %(max_treatment_rate_bbls_min)s, %(details)s)
    ON DUPLICATE KEY UPDATE
      date_simulated=VALUES(date_simulated),
      stimulated_formation=VALUES(stimulated_formation),
      type_treatment=VALUES(type_treatment),
      acid_pct=VALUES(acid_pct),
      lbs_proppant=VALUES(lbs_proppant),
      top_ft=VALUES(top_ft),
      bottom_ft=VALUES(bottom_ft),
      stimulation_stages=VALUES(stimulation_stages),
      volume=VALUES(volume), volume_units=VALUES(volume_units),
      max_pressure_psi=VALUES(max_pressure_psi),
      max_treatment_rate_bbls_min=VALUES(max_treatment_rate_bbls_min),
      details=VALUES(details);
    """
    cur = conn.cursor()
    cur.execute(sql, row); cur.close()

def write_bad_rows(path: str, rows: List[Dict[str, Any]]):
    if not rows:
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    keys = set()
    for r in rows: keys.update(r.keys())
    keys = list(keys)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows: w.writerow(r)
    print(f"[DIAG] bad rows exported -> {path}")

def process_header_csv(path: str, limit: Optional[int], verbose: bool) -> Tuple[List[Dict[str, Any]], Dict[str,int], List[Dict[str,Any]]]:
    f, reader = open_and_sniff(path)
    bad_rows = []
    stats = {"rows":0, "lat_none":0, "lon_none":0, "lat_bad":0, "lon_bad":0}
    rows_out = []
    with f:
        header = reader.fieldnames or []
        lat_col = find_col_name(header, "lat")
        lon_col = find_col_name(header, "lon")
        if verbose:
            print(f"[HEADERS] {header}")
            print(f"[MATCH] latitude col -> {lat_col}; longitude col -> {lon_col}")
        for i, row in enumerate(reader, start=2):
            if limit and stats["rows"] >= limit: break
            stats["rows"] += 1

            lat_raw = row.get(lat_col) if lat_col else None
            lon_raw = row.get(lon_col) if lon_col else None
            lat = to_decimal(lat_raw)
            lon = to_decimal(lon_raw)

            if lat_raw is None or str(lat_raw).strip()=="":
                stats["lat_none"] += 1
            if lon_raw is None or str(lon_raw).strip()=="":
                stats["lon_none"] += 1
            if lat_raw not in (None,"") and lat is None:
                stats["lat_bad"] += 1
            if lon_raw not in (None,"") and lon is None:
                stats["lon_bad"] += 1

            row["latitude"] = lat
            row["longitude"] = lon

            rows_out.append(row)

            if (lat_raw not in (None,"") and lat is None) or (lon_raw not in (None,"") and lon is None):
                r = dict(row)
                r["_line"] = i
                bad_rows.append(r)
    return rows_out, stats, bad_rows

def process_stim_csv(path: str, limit: Optional[int]) -> Tuple[List[Dict[str, Any]], Dict[str,int]]:
    f, reader = open_and_sniff(path)
    stats = {"rows":0}
    rows_out = []
    with f:
        for _i, row in enumerate(reader, start=2):
            if limit and stats["rows"] >= limit: break
            stats["rows"] += 1
            rows_out.append(row)
    return rows_out, stats

def run(header_csv: str, stim_csv: str, dry_run: bool, limit: Optional[int], verbose: bool):
    if not os.path.exists(header_csv):
        print(f"[ERR] header CSV not found: {header_csv}"); sys.exit(1)
    if not os.path.exists(stim_csv):
        print(f"[ERR] stim CSV not found: {stim_csv}"); sys.exit(1)

    # header
    header_rows, h_stats, h_bad = process_header_csv(header_csv, limit, verbose)
    print(f"[REPORT] well_header: rows={h_stats['rows']}, "
          f"lat_none={h_stats['lat_none']}, lon_none={h_stats['lon_none']}, "
          f"lat_bad={h_stats['lat_bad']}, lon_bad={h_stats['lon_bad']}")
    write_bad_rows("bad_rows_header.csv", h_bad)

    # stim
    stim_rows, s_stats = process_stim_csv(stim_csv, limit)
    print(f"[REPORT] well_stimulation: rows={s_stats['rows']}")

    if dry_run:
        print("[OK] Dry-run completed. No database writes.")
        return

    # 真正写库
    _ensure_mysql()
    import mysql.connector
    conn = mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS
    )
    try:
        init_db(conn)
        conn.database = DB_NAME

        ok_h = ok_s = 0
        for row in header_rows:
            upsert_header(conn, row); ok_h += 1
        for row in stim_rows:
            upsert_stimulation(conn, row); ok_s += 1
        conn.commit()
        print(f"[OK] DB import done. header={ok_h}, stim={ok_s}")
    finally:
        conn.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--header", required=True)
    ap.add_argument("--stim", required=True)
    ap.add_argument("--dry-run", action="store_true", help="parse & validate only, no DB writes")
    ap.add_argument("--limit", type=int, default=None, help="process only first N rows")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()
    run(args.header, args.stim, args.dry_run, args.limit, args.verbose)

if __name__ == "__main__":
    main()
