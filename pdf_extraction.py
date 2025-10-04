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


def clean_num(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    # keep digits, comma, dot
    s = re.sub(r"[^0-9\.,\-]", "", s)
    # collapse repeated commas
    s = re.sub(r",(?!\d)", "", s)
    return s.strip() or None


def value_inline(label_regex: str, text: str) -> Optional[str]:
    """
    Capture value on the SAME line after label.
    e.g. 'Maximum Treatment Pressure (PSI)  9679'
    """
    pat = re.compile(label_regex + r"\s*[:\-]?\s*([^\n\r]+)", re.I)
    m = pat.search(text)
    return m.group(1).strip() if m else None

def value_next_line(label_regex: str, text: str) -> Optional[str]:
    """
    Capture value on the NEXT non-empty line after a label line.
    e.g.
      Volume Units
      Barrels
    """
    # find the label line
    lab = re.compile(label_regex + r"\s*$", re.I | re.M)
    m = lab.search(text)
    if not m:
        return None
    # slice from end of label line
    tail = text[m.end():]
    # first non-empty line
    for line in tail.splitlines():
        line = line.strip()
        if line:
            return line
    return None


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


# --------- tolerant helpers for "label on one line, value on next" ---------
NUM = r"([0-9][0-9,]*(?:\.[0-9]+)?)"  # number with commas/decimals

def value_after(label_regex: str, text: str, numlike: bool = False) -> Optional[str]:
    """
    Find label (case-insensitive), then capture value on the same line
    or the next line. If numlike=True, only capture a number-looking token.
    """
    lab = re.compile(label_regex, re.I)
    for m in lab.finditer(text):
        tail = text[m.end():]
        lines = tail.splitlines()
        same = lines[0] if lines else ""
        nxt  = lines[1] if len(lines) > 1 else ""

        if numlike:
            m1 = re.search(NUM, same)
            if m1: return m1.group(1)
            m2 = re.search(NUM, nxt)
            if m2: return m2.group(1)
        else:
            m1 = re.search(r"([^\r\n]+)", same)
            if m1:
                v = m1.group(1).strip()
                if v: return v
            if nxt:
                m2 = re.match(r"([^\r\n]+)", nxt)
                if m2:
                    v2 = m2.group(1).strip()
                    if v2: return v2
    return None

def clean_num(s: Optional[str]) -> Optional[str]:
    if not s: return None
    return re.sub(r"[^\d.]", "", s)

def first_block_after(label_regex: str, text: str, max_chars: int = 300) -> Optional[str]:
    """
    Grab several lines after a 'Details' like label, stopping at blank/line of dashes.
    """
    m = re.search(label_regex + r"[:#]?\s*([\s\S]{0," + str(max_chars) + r"})", text, re.I)
    if not m: return None
    block = m.group(1)
    out_lines = []
    for ln in block.splitlines():
        s = ln.strip()
        if not s: break
        if re.fullmatch(r"[-_]{3,}", s): break
        out_lines.append(s)
    return "\n".join(out_lines) if out_lines else None


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
# RX_VOLUME_BLOCK  = re.compile(r"\bVolume\s*\n\s*([0-9,]+)[^\n]*\n.*?\bVolume\s*Units\s*\n\s*([A-Za-z]+)", re.I | re.S)
RX_VOLUME_BLOCK = re.compile(r"\bVolume\s*\n\s*([0-9][0-9,\.]*)\s*$\s*^Volume\s*Units\s*\n\s*([A-Za-z/]+)\s*$", re.I | re.M)
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
    stimulated_in: Optional[str] = None
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

def _find_1line(pat: re.Pattern, t: str):
    m = pat.search(t)
    return m.group(1).strip() if m else None

def _find_nextline(label: str, t: str):
    pat = re.compile(rf"{re.escape(label)}\s*\n\s*([0-9,.\-A-Za-z/ ]+)", re.I)
    m = pat.search(t)
    return m.group(1).strip() if m else None


PAT_DATE_SIM = re.compile(r"Date Stimulated\s*\n\s*(\d{1,2}/\d{1,2}/\d{4})", re.I)
PAT_FORMATION = re.compile(r"Stimulated Formation\s*\n\s*([^\n]+)", re.I)
PAT_TOP_BOTTOM_STAGES = re.compile(r"Top \(Ft\)\s*Bottom \(Ft\)\s*Stimulation Stages\n\s*(\d+)\s+(\d+)\s+(\d+)", re.I)
PAT_PSI = re.compile(r"Maximum Treatment Pressure \(PSI\)\s*\n\s*(\d+)", re.I)
PAT_LBS = re.compile(r"Lbs Proppant\s*\n\s*(\d+)", re.I)
PAT_TYPE_TREATMENT = re.compile(r"Type Treatment\s*\n\s*([^\n]+)", re.I)
PAT_VOLUME = re.compile(r"Volume Units\s*\n(\d+)\s*(\w+)", re.I)  # 同时提数值与单位
PAT_MAX_RATE = re.compile(r"Maximum Treatment Rate \(BBLS/Min\)\s*\n\s*(\d+(?:\.\d+)?)", re.I)
PAT_DETAILS = re.compile(r"\bDetails\b\s*\n([\s\S]{0,800})", re.I)

def _find(pat: re.Pattern, t: str) -> Optional[str]:
    m = pat.search(t)
    return m.group(1).strip() if m else None


def parse_stimulation(pages: List[str], pdf_name: str) -> StimRow:
    later = "\n".join(pages[2:]) if len(pages) > 2 else ""
    full = later if later.strip() else "\n".join(pages)

    t = full

    out = StimRow(pdf_name=pdf_name)

    # Date Stimulated  Stimulated Formation  Top (Ft)  Bottom (Ft)  Stimulation Stages  Volume  Volume Units
    header_pat = re.compile(
        r"Date\s*Stimulated\s+Stimulated\s*Formation\s+Top\s*\(Ft\)\s+Bottom\s*\(Ft\)\s+Stimulation\s*Stages\s+Volume\s+Volume\s*Units",
        re.I
    )
    m = header_pat.search(t)
    if m:
        after = t[m.end():]
        vals_line = None
        for line in after.splitlines():
            s = line.strip()
            if not s:
                continue
            vals_line = s
            break

        if vals_line:
            cols = re.split(r"\s{2,}", vals_line)
            if len(cols) < 7:
                cols = re.split(r"\s{1,}\|\s{1,}|\s{3,}", vals_line)

            # date, formation, top, bottom, stages, volume, units
            if len(cols) >= 7:
                out.date_simulated       = re.search(r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}", cols[0]).group(0) if re.search(r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}", cols[0]) else cols[0].strip()
                out.stimulated_formation = cols[1].strip()
                out.top_ft               = clean_num(cols[2])
                out.bottom_ft            = clean_num(cols[3])
                out.stimulation_stages   = clean_num(cols[4])
                out.volume               = clean_num(cols[5])
                # 单位只留字母
                units = re.sub(r"[^A-Za-z/]", "", cols[6]).strip()
                out.volume_units         = units or None

    if not out.date_simulated:
        out.date_simulated = (
            value_inline(r"Date\s*Stimulated", t) or
            value_next_line(r"Date\s*Stimulated", t) or
            first_or_none(RX_DATE_STIM, t)
        )
        if out.date_simulated:
            m = re.search(r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}", out.date_simulated)
            out.date_simulated = m.group(0) if m else out.date_simulated

    if not out.stimulated_formation:
        out.stimulated_formation = (
            value_inline(r"Stimulated\s*Formation", t) or
            value_next_line(r"Stimulated\s*Formation", t) or
            first_or_none(RX_FORMATION, t)
        )

    if not out.type_treatment:
        out.type_treatment = (
            value_inline(r"Type\s*Treatment", t) or
            value_next_line(r"Type\s*Treatment", t) or
            first_or_none(RX_TYPE_TREAT, t)
        )

    if not out.acid_pct:
        out.acid_pct = clean_num(
            value_inline(r"Acid\s*%", t) or value_next_line(r"Acid\s*%", t) or first_or_none(RX_ACID_PCT, t)
        )

    if not out.lbs_proppant:
        out.lbs_proppant = clean_num(
            value_inline(r"Lbs\s*Proppant", t) or value_next_line(r"Lbs\s*Proppant", t) or first_or_none(RX_LBS_PROP, t)
        )

    if not out.top_ft:
        out.top_ft = clean_num(value_inline(r"Top\s*\(Ft\)", t) or value_next_line(r"Top\s*\(Ft\)", t))
    if not out.bottom_ft:
        out.bottom_ft = clean_num(value_inline(r"Bottom\s*\(Ft\)", t) or value_next_line(r"Bottom\s*\(Ft\)", t))
    if not out.stimulation_stages:
        out.stimulation_stages = clean_num(value_inline(r"Stimulation\s*Stages", t) or value_next_line(r"Stimulation\s*Stages", t))

    if not (out.top_ft and out.bottom_ft and out.stimulation_stages):
        m = RX_TOP_BOT_STAGE.search(t)
        if m:
            a, b, c = [clean_num(x) for x in m.groups()]
            out.top_ft = out.top_ft or a
            out.bottom_ft = out.bottom_ft or b
            out.stimulation_stages = out.stimulation_stages or c

    # Volume & Units
    if not out.volume:
        out.volume = clean_num(value_inline(r"\bVolume\b", t) or value_next_line(r"\bVolume\b", t))
    if not out.volume_units:
        vu = value_inline(r"Volume\s*Units", t) or value_next_line(r"Volume\s*Units", t)
        if vu:
            vu = re.sub(r"[^A-Za-z/]", "", vu).strip()
            out.volume_units = vu or None
        if not out.volume_units:
            m = RX_VOLUME_BLOCK.search(t)
            if m:
                out.volume = clean_num(out.volume or m.group(1))
                out.volume_units = m.group(2)

    # Pressure/Rate
    if not out.max_pressure_psi:
        out.max_pressure_psi = clean_num(
            value_inline(r"Maximum\s*Treatment\s*Pressure\s*\(PSI\)", t) or
            value_next_line(r"Maximum\s*Treatment\s*Pressure\s*\(PSI\)", t) or
            first_or_none(RX_PRESS_PSI, t)
        )
    if not out.max_treatment_rate_bbls_min:
        out.max_treatment_rate_bbls_min = clean_num(
            value_inline(r"Maximum\s*Treatment\s*Rate\s*\(BBLS/?Min\)", t) or
            value_next_line(r"Maximum\s*Treatment\s*Rate\s*\(BBLS/?Min\)", t) or
            first_or_none(RX_MAX_RATE, t)
        )

    # Details
    if not out.details:
        det = value_next_line(r"\bDetails\b", t)
        if det and len(det) < 400:
            out.details = det

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
