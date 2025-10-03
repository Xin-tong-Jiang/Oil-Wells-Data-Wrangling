#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Extract fields from a folder of PDFs and write two CSVs:
  - well_header.csv
  - well_stimulation.csv

Usage:
  python extract_to_csv.py /path/to/pdfs \
    --out-header well_header.csv \
    --out-stim well_stimulation.csv \
    --dpi 300 --prefer-ocr
"""

import sys, re, csv
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional, List

# -------- Optional PDF/OCR deps (safe imports) --------
try:
    import pdfplumber
except Exception:
    pdfplumber = None

try:
    from pdf2image import convert_from_path
    import pytesseract
except Exception:
    convert_from_path = None
    pytesseract = None


# ============================== Utilities ==============================

def _norm(s: str) -> str:
    """Normalize punctuation and collapse spaces."""
    if not s:
        return ""
    s = (s.replace("º", "°").replace("˚", "°")
           .replace("’", "'").replace("′", "'")
           .replace("“", '"').replace("”", '"')
           .replace("—", "-").replace("–", "-")
           .replace("·", "."))
    return re.sub(r"[ \t]+", " ", s).strip()


def extract_pages_text(pdf_path: Path, dpi: int = 300, prefer_ocr: bool = False) -> List[str]:
    """Return per-page text. Prefer text-layer unless --prefer-ocr is set."""
    pages: List[str] = []

    def try_pdfplumber() -> List[str]:
        if not pdfplumber:
            return []
        try:
            with pdfplumber.open(str(pdf_path)) as pdf:
                return [_norm(p.extract_text() or "") for p in pdf.pages]
        except Exception as e:
            sys.stderr.write(f"[WARN] pdfplumber failed for {pdf_path.name}: {e}\n")
            return []

    def try_ocr() -> List[str]:
        if not (convert_from_path and pytesseract):
            return []
        try:
            imgs = convert_from_path(str(pdf_path), dpi=dpi)
            return [_norm(pytesseract.image_to_string(img, lang="eng") or "") for img in imgs]
        except Exception as e:
            sys.stderr.write(f"[WARN] OCR failed for {pdf_path.name}: {e}\n")
            return []

    if prefer_ocr:
        pages = try_ocr()
        if not any(p.strip() for p in pages):
            pages = try_pdfplumber()
    else:
        pages = try_pdfplumber()
        if not any(p.strip() for p in pages):
            pages = try_ocr()

    return pages


def dms_to_decimal(dms: Optional[str]) -> Optional[float]:
    """Convert DMS to decimal degrees; supports H° M' S\" H and plain decimals with hemisphere."""
    if not dms:
        return None
    s = _norm(dms).upper()

    # Plain decimal with optional hemisphere (e.g., 103.73 W)
    m = re.fullmatch(r"([NSWE]?)\s*(-?\d+(?:\.\d+)?)\s*([NSWE]?)", s)
    if m:
        val = float(m.group(2))
        hemi = (m.group(1) or m.group(3) or "")
        if hemi in ("S", "W"):
            val = -abs(val)
        return val

    # D M S with optional hemisphere
    m = re.search(r"([NSWE]?)\s*(\d{1,3})[° ]\s*(\d{1,2})[' ]\s*(\d{1,2}(?:\.\d+)?)\"?\s*([NSWE]?)", s)
    if not m:
        return None
    hemi1, deg, minu, sec, hemi2 = m.groups()
    val = float(deg) + float(minu)/60.0 + float(sec)/3600.0
    hemi = hemi1 or hemi2 or ""
    if hemi in ("S", "W"):
        val = -abs(val)
    return val


def first_or_none(pattern: re.Pattern, text: str) -> Optional[str]:
    m = pattern.search(text)
    return m.group(1).strip() if m else None


# ============================== Regex (Header) ==============================

RX_OPERATOR      = re.compile(r"(?:\bWell\s+Operator|\bOperator|Responsible\s+Party)\s*[:：\-]\s*([^\n\r]+)", re.I)
RX_WELLNAME      = re.compile(r"\bWell\s*(?:Name|(?:or\s*Facility)?\s*Name)\s*[:：\-]\s*([^\n\r]+)", re.I)
RX_API           = re.compile(r"(?:API\s*(?:#|No\.?)?|Well\s*File\s*No\.?)\s*[:\-]?\s*([0-9]{5,}|\d{2}\s*-\s*\d{3}\s*-\s*\d{5})", re.I)
RX_ENSECO        = re.compile(r"\bEnseco\s*Job#?\s*[:：#]?\s*([A-Z]?\d[\w\-]*)", re.I)
RX_JOBTYPE       = re.compile(r"\bJob\s*Type\s*[:：\-]\s*([^\n\r]+)", re.I)
RX_COUNTY_STATE  = re.compile(r"\bCounty\s*,\s*State\s*[:：\-]\s*([^\n\r]+)", re.I)
RX_SHL           = re.compile(r"Well\s*Surface\s*Hole\s*Location\s*\(SHL\)\s*[:：\-]\s*([^\n\r]+)", re.I)
RX_LAT           = re.compile(r"\bLatitude\s*[:：\-]\s*([^\n\r]+)", re.I)
RX_LON           = re.compile(r"\bLongitude\s*[:：\-]\s*([^\n\r]+)", re.I)
RX_DATUM         = re.compile(r"\bDatum\s*[:：\-]\s*([^\n\r]+)", re.I)

# ============================== Regex (Stimulation) ==============================

RX_DATE_STIM     = re.compile(r"Date\s*Stimulated\s*\n\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})", re.I)
RX_FORMATION     = re.compile(r"Stimulated\s*Formation\s*\n\s*([^\n]+)", re.I)
RX_TYPE_TREAT    = re.compile(r"Type\s*Treatment\s*\n\s*([^\n]+)", re.I)
RX_ACID_PCT      = re.compile(r"Acid\s*%[\s\S]*?\n\s*([0-9.]+)", re.I)
RX_LBS_PROP      = re.compile(r"Lbs\s*Proppant\s*\n\s*([0-9,]+)", re.I)
RX_TOP_BOT_STAGE = re.compile(r"Top\s*\(Ft\)\s*Bottom\s*\(Ft\)\s*Stimulation\s*Stages\s*\n\s*([0-9,]+)\s+([0-9,]+)\s+([0-9,]+)", re.I)
RX_PRESS_PSI     = re.compile(r"Maximum\s*Treatment\s*Pressure\s*\(PSI\)\s*\n\s*([0-9,]+)", re.I)
RX_MAX_RATE      = re.compile(r"Maximum\s*Treatment\s*Rate\s*\(BBLS/Min\)\s*\n\s*([0-9]+(?:\.[0-9]+)?)", re.I)
RX_VOLUME_BLOCK  = re.compile(r"\bVolume\s*\n\s*([0-9,]+)[^\n]*\n.*?\bVolume\s*Units\s*\n\s*([A-Za-z]+)", re.I | re.S)
RX_DETAILS       = re.compile(r"\bDetails\b[:#]?\s*([^\n\r]+(?:\n[^\n\r]+){0,5})", re.I)


# ============================== Data Models ==============================

@dataclass
class HeaderRow:
    pdf_name: str
    operator: Optional[str] = None
    well_name: Optional[str] = None
    api: Optional[str] = None
    enseco_job: Optional[str] = None
    job_type: Optional[str] = None
    county_state: Optional[str] = None
    shl: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    datum: Optional[str] = None


@dataclass
class StimRow:
    pdf_name: str
    date_simulated: Optional[str] = None
    stimulated_formation: Optional[str] = None
    type_treatment: Optional[str] = None
    acid_pct: Optional[str] = None
    lbs_proppant: Optional[str] = None
    top_ft: Optional[str] = None
    bottom_ft: Optional[str] = None
    stimulation_stages: Optional[str] = None
    volume: Optional[str] = None
    volume_units: Optional[str] = None
    max_pressure_psi: Optional[str] = None
    max_treatment_rate_bbls_min: Optional[str] = None
    details: Optional[str] = None


# ============================== Parsing ==============================

def normalize_api(api: Optional[str]) -> Optional[str]:
    """Format as 33-053-06057 if possible."""
    if not api:
        return None
    s = re.sub(r"\s", "", api)
    m = re.search(r"(\d{2})-?(\d{3})-?(\d{5})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return s


def parse_header(pages: List[str], pdf_name: str) -> HeaderRow:
    # Header info is typically on page 1–2
    text = "\n".join(pages[:2]) if len(pages) >= 2 else "\n".join(pages)
    operator      = first_or_none(RX_OPERATOR, text)
    well_name     = first_or_none(RX_WELLNAME, text)
    api           = normalize_api(first_or_none(RX_API, text))
    enseco_job    = first_or_none(RX_ENSECO, text)
    job_type      = first_or_none(RX_JOBTYPE, text)
    county_state  = first_or_none(RX_COUNTY_STATE, text)
    shl           = first_or_none(RX_SHL, text)
    lat_raw       = first_or_none(RX_LAT, text)
    lon_raw       = first_or_none(RX_LON, text)
    latitude      = dms_to_decimal(lat_raw) if lat_raw else None
    longitude     = dms_to_decimal(lon_raw) if lon_raw else None
    datum         = first_or_none(RX_DATUM, text)

    return HeaderRow(
        pdf_name=pdf_name,
        operator=operator,
        well_name=well_name,
        api=api,
        enseco_job=enseco_job,
        job_type=job_type,
        county_state=county_state,
        shl=shl,
        latitude=latitude,
        longitude=longitude,
        datum=datum,
    )


def parse_stimulation(pages: List[str], pdf_name: str) -> StimRow:
    # Stimulation block tends to be on later pages, but fall back to full text
    later = "\n".join(pages[2:]) if len(pages) > 2 else ""
    t = later if later.strip() else "\n".join(pages)

    out = StimRow(pdf_name=pdf_name)
    out.date_simulated       = first_or_none(RX_DATE_STIM, t)
    out.stimulated_formation = first_or_none(RX_FORMATION, t)
    out.type_treatment       = first_or_none(RX_TYPE_TREAT, t)
    out.acid_pct             = first_or_none(RX_ACID_PCT, t)
    out.lbs_proppant         = first_or_none(RX_LBS_PROP, t)
    if m := RX_TOP_BOT_STAGE.search(t):
        out.top_ft, out.bottom_ft, out.stimulation_stages = [g.replace(",", "") for g in m.groups()]
    if m := RX_VOLUME_BLOCK.search(t):
        out.volume, out.volume_units = m.groups()
        out.volume = out.volume.replace(",", "")
    out.max_pressure_psi           = (first_or_none(RX_PRESS_PSI, t) or "").replace(",", "") or None
    out.max_treatment_rate_bbls_min= first_or_none(RX_MAX_RATE, t)
    out.details                    = first_or_none(RX_DETAILS, t)
    return out


# ============================== Runner ==============================

def process_folder(folder: Path, out_header: Path, out_stim: Path, dpi: int = 300, prefer_ocr: bool = False):
    pdfs = sorted(folder.rglob("*.pdf"))
    if not pdfs:
        print("No PDFs found.")
        return

    # Prepare writers
    header_fields = list(asdict(HeaderRow(pdf_name="__dummy__")).keys())
    stim_fields   = list(asdict(StimRow(pdf_name="__dummy__")).keys())

    with open(out_header, "w", newline="", encoding="utf-8") as f_h, \
         open(out_stim,   "w", newline="", encoding="utf-8") as f_s:

        w_h = csv.DictWriter(f_h, fieldnames=header_fields)
        w_s = csv.DictWriter(f_s, fieldnames=stim_fields)
        w_h.writeheader()
        w_s.writeheader()

        for pdf in pdfs:
            print(f"[INFO] {pdf.name}")
            pages = extract_pages_text(pdf, dpi=dpi, prefer_ocr=prefer_ocr)
            if not any(p.strip() for p in pages):
                print(f"[WARN] No text extracted: {pdf.name}", file=sys.stderr)
                continue

            header_row = parse_header(pages, pdf.name)
            stim_row   = parse_stimulation(pages, pdf.name)

            w_h.writerow(asdict(header_row))
            w_s.writerow(asdict(stim_row))

    print(f"[DONE] {len(pdfs)} PDFs processed.")
    print(f"  - well_header CSV:      {out_header}")
    print(f"  - well_stimulation CSV: {out_stim}")


# ============================== CLI ==============================

def main():
    import argparse
    p = argparse.ArgumentParser("Extract PDFs → well_header.csv & well_stimulation.csv")
    p.add_argument("folder", type=str, help="Folder containing PDFs")
    p.add_argument("--out-header", type=str, default="well_header.csv", help="Output CSV for header fields")
    p.add_argument("--out-stim",   type=str, default="well_stimulation.csv", help="Output CSV for stimulation fields")
    p.add_argument("--dpi",        type=int, default=300, help="OCR render DPI if OCR is used")
    p.add_argument("--prefer-ocr", action="store_true", help="Prefer OCR first (default prefers text-layer)")
    args = p.parse_args()

    folder = Path(args.folder).expanduser().resolve()
    out_header = Path(args.out_header).expanduser().resolve()
    out_stim   = Path(args.out_stim).expanduser().resolve()

    out_header.parent.mkdir(parents=True, exist_ok=True)
    out_stim.parent.mkdir(parents=True, exist_ok=True)

    process_folder(folder, out_header, out_stim, dpi=args.dpi, prefer_ocr=args.prefer_ocr)


if __name__ == "__main__":
    main()
