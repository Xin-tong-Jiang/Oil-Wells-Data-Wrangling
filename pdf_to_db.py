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

import os
import csv
import argparse
from decimal import Decimal, InvalidOperation
from typing import Tuple, Dict, Any

import mysql.connector
from dotenv import load_dotenv

load_dotenv()

# -------------------- DB Config --------------------
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER", "phpmyadmin")
DB_PASS = os.getenv("DB_PASS", "root")
DB_NAME = os.getenv("DB_NAME", "oilwell_pdf_extraction")


# -------------------- DB Schema --------------------
def init_db() -> None:
    """Create database and tables if needed."""
    conn = mysql.connector.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS)
    cur = conn.cursor()
    cur.execute(
        f"CREATE DATABASE IF NOT EXISTS {DB_NAME} "
        f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    )
    cur.execute(f"USE {DB_NAME}")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS well_header (
            pdf_name            VARCHAR(255) PRIMARY KEY,
            operator            VARCHAR(255),
            well_name           VARCHAR(255),
            api                 VARCHAR(32),
            enseco_job          VARCHAR(64),
            job_type            VARCHAR(128),
            county_state        VARCHAR(256),
            shl                 TEXT,
            latitude            DECIMAL(12,9),
            longitude           DECIMAL(12,9),
            datum               VARCHAR(128)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS well_stimulation (
            pdf_name                    VARCHAR(255) PRIMARY KEY,
            date_simulated              VARCHAR(32),
            stimulated_formation        VARCHAR(128),
            type_treatment              VARCHAR(128),
            acid_pct                    VARCHAR(32),
            lbs_proppant                VARCHAR(32),
            top_ft                      VARCHAR(32),
            bottom_ft                   VARCHAR(32),
            stimulation_stages          VARCHAR(32),
            volume                      VARCHAR(32),
            volume_units                VARCHAR(32),
            max_pressure_psi            VARCHAR(32),
            max_treatment_rate_bbls_min VARCHAR(32),
            details                     TEXT,
            updated_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                                         ON UPDATE CURRENT_TIMESTAMP,
            CONSTRAINT fk_stim_pdf
                FOREIGN KEY (pdf_name) REFERENCES well_header(pdf_name)
                ON DELETE CASCADE ON UPDATE CASCADE
        )
    """)
    conn.commit()
    cur.close()
    conn.close()


# -------------------- Helpers --------------------
def to_decimal(val) -> Decimal | None:
    """
    Convert messy numeric strings to Decimal safely.
    Handles Unicode minus (−/–/—), commas, quotes, blanks, NA tokens.
    Returns None if not convertible.
    """
    if val is None:
        return None
    s = str(val).strip()
    if s == "" or s.lower() in {"na", "n/a", "null", "none"}:
        return None
    s = (s.replace("−", "-")
           .replace("–", "-")
           .replace("—", "-"))
    s = s.replace(",", "")
    s = s.strip('"').strip("'")
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def open_and_sniff(path: str) -> Tuple[Any, csv.DictReader]:
    """
    Open CSV file and sniff delimiter. Prefer utf-8-sig; fallback latin-1.
    Returns (file_handle, DictReader).
    """
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


def normalize_keys(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize CSV header names to lower-case, trimmed keys.
    (If你的CSV列名大小写/空格不一致，打开此函数并在读取后调用。)
    """
    return { (k or "").strip().lower(): v for k, v in row.items() }


# -------------------- Upserts --------------------
def upsert_header(conn, row: Dict[str, Any]) -> None:
    sql = """
    INSERT INTO well_header
      (pdf_name, operator, well_name, api, enseco_job, job_type,
       county_state, shl, latitude, longitude, datum)
    VALUES
      (%(pdf_name)s, %(operator)s, %(well_name)s, %(api)s, %(enseco_job)s, %(job_type)s,
       %(county_state)s, %(shl)s, %(latitude)s, %(longitude)s, %(datum)s)
    ON DUPLICATE KEY UPDATE
      operator=VALUES(operator),
      well_name=VALUES(well_name),
      api=VALUES(api),
      enseco_job=VALUES(enseco_job),
      job_type=VALUES(job_type),
      county_state=VALUES(county_state),
      shl=VALUES(shl),
      latitude=VALUES(latitude),
      longitude=VALUES(longitude),
      datum=VALUES(datum);
    """
    cur = conn.cursor()
    cur.execute(sql, row)
    cur.close()


def upsert_stimulation(conn, row: Dict[str, Any]) -> None:
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
      volume=VALUES(volume),
      volume_units=VALUES(volume_units),
      max_pressure_psi=VALUES(max_pressure_psi),
      max_treatment_rate_bbls_min=VALUES(max_treatment_rate_bbls_min),
      details=VALUES(details);
    """
    cur = conn.cursor()
    cur.execute(sql, row)
    cur.close()


# -------------------- Load CSV → DB --------------------
def load_csvs_to_db(header_csv: str, stim_csv: str) -> None:
    init_db()
    conn = mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, database=DB_NAME
    )

    header_ok = 0
    header_err = 0
    stim_ok = 0
    stim_err = 0

    try:
        # ------- well_header.csv -------
        if not os.path.exists(header_csv):
            raise FileNotFoundError(f"Header CSV not found: {header_csv}")
        f1, reader1 = open_and_sniff(header_csv)
        with f1:
            for i, row in enumerate(reader1, start=2):  # data starts at line 2
                try:
                    # 若需要统一列名大小写，取消下一行注释：
                    # row = normalize_keys(row)

                    row["latitude"] = to_decimal(row.get("latitude"))
                    row["longitude"] = to_decimal(row.get("longitude"))
                    upsert_header(conn, row)
                    header_ok += 1
                except Exception as e:
                    header_err += 1
                    print(f"[header line {i}] ERROR: {e} | row={row}")

        # ------- well_stimulation.csv -------
        if not os.path.exists(stim_csv):
            raise FileNotFoundError(f"Stimulation CSV not found: {stim_csv}")
        f2, reader2 = open_and_sniff(stim_csv)
        with f2:
            for i, row in enumerate(reader2, start=2):
                try:
                    # row = normalize_keys(row)  # 若需要
                    upsert_stimulation(conn, row)
                    stim_ok += 1
                except Exception as e:
                    stim_err += 1
                    print(f"[stim line {i}] ERROR: {e} | row={row}")

        conn.commit()
        print(f"[OK] Import done. header: {header_ok} ok, {header_err} err; "
              f"stim: {stim_ok} ok, {stim_err} err.")
    finally:
        conn.close()


# -------------------- CLI --------------------
def main():
    parser = argparse.ArgumentParser("Load CSVs into MySQL")
    parser.add_argument("--header", required=True, help="well_header.csv path")
    parser.add_argument("--stim", required=True, help="well_stimulation.csv path")
    args = parser.parse_args()

    load_csvs_to_db(args.header, args.stim)


if __name__ == "__main__":
    main()

