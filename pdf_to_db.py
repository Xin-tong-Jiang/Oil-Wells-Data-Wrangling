#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Load CSVs (well_header.csv & well_stimulation.csv) into MySQL database.

Usage:
  python csv_to_db.py \
    --header well_header.csv \
    --stim well_stimulation.csv
"""

import os, csv
import mysql.connector
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

# -------------------- DB Config --------------------
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER", "phpmyadmin")
DB_PASS = os.getenv("DB_PASS", "root")
DB_NAME = os.getenv("DB_NAME", "oilwell_pdf_extraction")

# -------------------- DB Schema --------------------
def init_db():
    """Create database and tables if needed."""
    conn = mysql.connector.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS)
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
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
            updated_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            CONSTRAINT fk_stim_pdf
                FOREIGN KEY (pdf_name) REFERENCES well_header(pdf_name)
                ON DELETE CASCADE ON UPDATE CASCADE
        )
    """)

    conn.commit()
    cur.close()
    conn.close()

# -------------------- Load CSV → DB --------------------
def upsert_header(conn, row: dict):
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

def upsert_stimulation(conn, row: dict):
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

def load_csvs_to_db(header_csv: str, stim_csv: str):
    init_db()
    conn = mysql.connector.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, database=DB_NAME)

    try:
        # Load well_header.csv
        with open(header_csv, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row["latitude"] = float(row["latitude"]) if row.get("latitude") else None
                row["longitude"] = float(row["longitude"]) if row.get("longitude") else None
                upsert_header(conn, row)

        # Load well_stimulation.csv
        with open(stim_csv, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                upsert_stimulation(conn, row)

        conn.commit()
        print("[OK] Data imported successfully.")
    finally:
        conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser("Load CSVs into MySQL")
    parser.add_argument("--header", required=True, help="well_header.csv path")
    parser.add_argument("--stim", required=True, help="well_stimulation.csv path")
    args = parser.parse_args()

    load_csvs_to_db(args.header, args.stim)
