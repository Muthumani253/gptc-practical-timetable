#!/usr/bin/env python3
# extract_pdf.py  — department mapping embedded (no JSON)
# Writes:
#  - data/extracted/PracticalMaster.csv
#  - data/extracted/StudentSubjectMap.csv

import re
import os
import sys
from collections import Counter
from datetime import datetime

import pdfplumber
import pandas as pd

# Ensure stdout uses utf-8 on Windows / Streamlit Cloud logs
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

# --- Paths ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
INPUT_DIR = os.path.join(DATA_DIR, "input_pdf")
EXTRACTED_DIR = os.path.join(DATA_DIR, "extracted")

os.makedirs(EXTRACTED_DIR, exist_ok=True)
os.makedirs(INPUT_DIR, exist_ok=True)

# ----------------------------------------
# Department Mapping (NCNO → Dept Name)
# ----------------------------------------
DEPT_MAP = {
    "101": "CIVIL ENGINEERING",
    "102": "MECHANICAL ENGINEERING",
    "103": "ELECTRICAL & ELECTRONICS ENGINEERING",
    "104": "ELECTRONICS & COMMUNICATION ENGINEERING",
    "105": "COMPUTER ENGINEERING",
    "106": "INSTRUMENTATION & CONTROL ENGINEERING",
    "107": "AUTOMOBILE ENGINEERING",
    "108": "PRODUCTION ENGINEERING",
    "109": "TEXTILE TECHNOLOGY",
    "110": "PRINTING TECHNOLOGY",
    "111": "LEATHER TECHNOLOGY",
    "112": "CHEMICAL ENGINEERING",
    "113": "CERAMIC TECHNOLOGY",
    "114": "AGRICULTURAL TECHNOLOGY",
    "115": "ARCHITECTURE ASSISTANTSHIP",
    "116": "MARINE ENGINEERING",
    "117": "MECHATRONICS ENGINEERING",
    "118": "PETROCHEMICAL ENGINEERING",
    "119": "ROBOTICS & AUTOMATION",
    "120": "TOOL & DIE ENGINEERING",
    "121": "INFORMATION TECHNOLOGY",
    "122": "ARTIFICIAL INTELLIGENCE & MACHINE LEARNING",
    "123": "CLOUD COMPUTING & BIG DATA",
    "124": "ARTIFICIAL INTELLIGENCE & DATA SCIENCE",
    "125": "REFRIGERATION & AIR CONDITIONING",
    "201": "CIVIL ENGINEERING (SANDWICH)",
    "202": "MECHANICAL ENGINEERING (SANDWICH)",
    "203": "ELECTRICAL & ELECTRONICS ENGINEERING (SANDWICH)",
    "209": "TEXTILE MANUFACTURING",
    "210": "APPAREL TECHNOLOGY",
    "211": "PRINTING TECHNOLOGY (DIGITAL PRINTING)",
    "301": "COMPUTER NETWORKING",
    "302": "CLOUD COMPUTING",
    "303": "INTERNET OF THINGS (IoT)",
    "304": "ARTIFICIAL INTELLIGENCE",
    "305": "CYBER SECURITY",
    "401": "COMMERCIAL PRACTICE",
    "402": "MODERN OFFICE PRACTICE",
    "403": "LIBRARY & INFORMATION SCIENCE",
    "404": "HOTEL MANAGEMENT & CATERING TECHNOLOGY",
    "405": "GARMENT TECHNOLOGY",
    "406": "FASHION DESIGN & TECHNOLOGY"
}

# --- Helpers ---

def parse_institution(text):
    """Try to read ins_code and institute_line from page-1 header (best-effort)."""
    ins_code, institute_line = "", ""
    m = re.search(r"Ins\s*Code\s*Name\s*of\s*the\s*Institution\s*\n+(\d{2,4})\s+([^\n]+)", text, flags=re.IGNORECASE)
    if m:
        ins_code, institute_line = m.group(1).strip(), m.group(2).strip()
    else:
        # fallback: any leading "<code> GOVERNMENT POLYTECHNIC" line
        m2 = re.search(r"^(\d{2,4})\s*,?\s*GOVERNMENT\s+POLYTECHNIC\s+COLLEGE.*", text, flags=re.IGNORECASE | re.MULTILINE)
        if m2:
            ins_code = m2.group(1).strip()
            # try to extract whole line as institute_line
            line = m2.group(0).strip()
            institute_line = line
    return ins_code, institute_line

def detect_page_kind(text):
    if re.search(r"PRACTICAL\s+CHECK\s+LIST\s*\(SUMMARY\)", text, flags=re.IGNORECASE):
        return "summary"
    if re.search(r"PRACTICAL\s+CHECK\s+LIST\s*::", text, flags=re.IGNORECASE):
        return "subject"
    return None

def extract_summary_rows(text):
    rows, started = [], False
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if re.search(r"SNo\s+NCNO\s+SubCode", line, flags=re.IGNORECASE):
            started = True
            continue
        if not started:
            continue
        parts = re.split(r"\s{1,}", line)
        if len(parts) < 6:
            continue
        try:
            s_no = parts[0]
            ncno = parts[1]
            sub_code = parts[2]
            maybe_type = parts[-2]
            noc = int(parts[-1])
            subject_name = " ".join(parts[3:-2]).strip()
            if not re.fullmatch(r"\d+", s_no):
                continue
            rows.append({
                "ncno": ncno,
                "sub_code": sub_code,
                "subject_name": subject_name,
                "type": maybe_type,
                "noc": noc
            })
        except Exception:
            continue
    return rows

def extract_subject_header(text):
    """Look for a line like: 123-1000-CH232451 APPLIED CHEMISTRY - I P"""
    for line in text.splitlines():
        line = line.strip()
        m = re.match(r"(\d{2,4}-\d{3,4}-[A-Z0-9\-]+)\s+(.+?)\s+(P|PT|ASC)\s*$", line)
        if m:
            return m.group(1), m.group(2).strip(), m.group(3)
    return None, None, None

def extract_student_rows(text):
    rows, started = [], False
    header_pat = re.compile(r"S\.?No\s+NCNO\s+Reg\s*No\s+Name.*DoB\s+Regl\s+Sem\s+Col", re.IGNORECASE)
    line_pat = re.compile(r"^\s*(\d+)\s+(\d{3,4})\s+(\d+)\s+(.+?)\s+(\d{2}\.\d{2}\.\d{4})\s+([A-Z0-9]+)\s+(\d+)\s+(\d+)\s*$")
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            if started:
                break
            continue
        if header_pat.search(line):
            started = True
            continue
        if started:
            m = line_pat.match(line)
            if m:
                try:
                    rows.append({
                        "ncno": m.group(2),
                        "reg_no": m.group(3),
                        "student_name": m.group(4).strip(),
                        "dob": m.group(5),
                        "regl": m.group(6),
                        "sem": int(m.group(7)),
                        "col_no": int(m.group(8))
                    })
                except Exception:
                    pass
            else:
                # fallback split if regex doesn't match (long names)
                parts = re.split(r"\s{1,}", line)
                if len(parts) >= 8:
                    try:
                        rows.append({
                            "ncno": parts[1],
                            "reg_no": parts[2],
                            "student_name": " ".join(parts[3:-4]).strip(),
                            "dob": parts[-4],
                            "regl": parts[-3],
                            "sem": int(parts[-2]),
                            "col_no": int(parts[-1])
                        })
                    except Exception:
                        pass
    return rows

def month_year_from_text(text):
    m = re.search(r"(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s+(\d{4})", text, flags=re.IGNORECASE)
    return f"{m.group(1)[:3].upper()} {m.group(2)}" if m else datetime.now().strftime("%b %Y").upper()

# --- Core extraction ---

def _dept_name_for_ncno(ncno: str):
    if not ncno:
        return "UNKNOWN DEPARTMENT (NCNO: )"
    nc = str(ncno).zfill(3) if len(str(ncno)) < 3 else str(ncno)
    dept = DEPT_MAP.get(str(ncno)) or DEPT_MAP.get(nc) or f"UNKNOWN DEPARTMENT (NCNO: {ncno})"
    return dept

def extract_all(pdf_path):
    practical_master = {}
    student_rows_all = []
    ins_code = ""
    exam_month_year = ""

    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        print(f"\n[INFO] Starting extraction from: {os.path.basename(pdf_path)}")
        print(f"[INFO] Total pages detected: {total_pages}\n")

        for pno, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            print(f"[PAGE {pno}/{total_pages}] Reading... ", end="")

            if pno == 1:
                ins_code, _ = parse_institution(text)
                exam_month_year = month_year_from_text(text)

            kind = detect_page_kind(text)
            if kind == "summary":
                print("Summary section found")
                rows = extract_summary_rows(text)
                for r in rows:
                    ncno = r["ncno"]
                    sub_code = r["sub_code"]
                    subject_name = r["subject_name"]
                    ptype = r["type"]
                    noc = r["noc"]
                    practical_code = f"{ins_code}-{ncno}-{sub_code}" if ins_code else f"{ncno}-{sub_code}"
                    practical_master[practical_code] = {
                        "ins_code": ins_code or "",
                        "ncno": ncno,
                        "dept_name": _dept_name_for_ncno(ncno),
                        "sub_code": sub_code,
                        "subject_name": subject_name,
                        "type": ptype,
                        "col_no": None,
                        "total_candidates": noc,
                        "practical_code": practical_code,
                        "exam_month_year": exam_month_year or "",
                    }

            elif kind == "subject":
                practical_code, subject_name_pg, ptype_pg = extract_subject_header(text)
                print(f"Subject section: {practical_code or 'Unknown'}")
                if not practical_code:
                    print("  -> missing practical code, skipping")
                    continue

                stud_rows = extract_student_rows(text)
                print(f"  -> {len(stud_rows)} students parsed from subject page")
                if not stud_rows:
                    continue

                # Use NCNO from first student row on that page to decide dept_name for all students on that page
                page_ncno = stud_rows[0].get("ncno", "")
                page_dept = _dept_name_for_ncno(page_ncno)

                col_no = None
                if stud_rows:
                    col_no = Counter([r["col_no"] for r in stud_rows]).most_common(1)[0][0]

                for s in stud_rows:
                    student_rows_all.append({
                        "reg_no": s["reg_no"],
                        "student_name": s["student_name"],
                        "dob": s["dob"],
                        "regl": s["regl"],
                        "sem": s["sem"],
                        "ncno": s["ncno"],
                        "dept_name": page_dept,
                        "sub_code": practical_code.split("-")[-1],
                        "subject_name": subject_name_pg or "",
                        "type": ptype_pg or "",
                        "col_no": s["col_no"],
                        "practical_code": practical_code,
                        "ins_code": practical_code.split("-")[0] if "-" in practical_code else (ins_code or ""),
                    })

                if practical_code not in practical_master:
                    practical_master[practical_code] = {
                        "ins_code": practical_code.split("-")[0] if "-" in practical_code else (ins_code or ""),
                        "ncno": page_ncno,
                        "dept_name": page_dept,
                        "sub_code": practical_code.split("-")[-1],
                        "subject_name": subject_name_pg or "",
                        "type": ptype_pg or "",
                        "col_no": col_no,
                        "total_candidates": len(stud_rows),
                        "practical_code": practical_code,
                        "exam_month_year": exam_month_year or "",
                    }
                else:
                    if practical_master[practical_code].get("col_no") is None and col_no is not None:
                        practical_master[practical_code]["col_no"] = col_no
                    if stud_rows:
                        practical_master[practical_code]["total_candidates"] = max(
                            practical_master[practical_code].get("total_candidates", 0), len(stud_rows)
                        )
            else:
                print("Skipped (no match)")

    pm_path = os.path.join(EXTRACTED_DIR, "PracticalMaster.csv")
    ssm_path = os.path.join(EXTRACTED_DIR, "StudentSubjectMap.csv")

    pm_cols = ["ins_code","ncno","dept_name","sub_code","subject_name","type","col_no","total_candidates","practical_code","exam_month_year"]
    ssm_cols = ["reg_no","student_name","dob","regl","sem","ncno","dept_name","sub_code","subject_name","type","col_no","practical_code","ins_code"]

    pm_df = pd.DataFrame([practical_master[k] for k in sorted(practical_master.keys())], columns=pm_cols) if practical_master else pd.DataFrame(columns=pm_cols)
    ssm_df = pd.DataFrame(student_rows_all, columns=ssm_cols) if student_rows_all else pd.DataFrame(columns=ssm_cols)

    pm_df.to_csv(pm_path, index=False, encoding="utf-8-sig")
    ssm_df.to_csv(ssm_path, index=False, encoding="utf-8-sig")

    with open(os.path.join(EXTRACTED_DIR, "extraction_log.txt"), "w", encoding="utf-8") as f:
        f.write(f"Extracted {len(pm_df)} practical(s), {len(ssm_df)} student rows\n")
        f.write(f"Exam: {exam_month_year}\n")
        f.write(f"PDF: {os.path.basename(pdf_path)}\n")

    print("\n[SUMMARY]")
    print(f"  Total practicals extracted : {len(pm_df)}")
    print(f"  Total student rows         : {len(ssm_df)}")
    print(f"  Exam Month/Year            : {exam_month_year}")
    print(f"\n[OK] Wrote: {pm_path}")
    print(f"[OK] Wrote: {ssm_path}")
    print(f"[OK] Log saved to: {os.path.join(EXTRACTED_DIR, 'extraction_log.txt')}")
    print("\n✅ Extraction complete.\n")

# --- Entry Point ---
if __name__ == "__main__":
    pdfs = [f for f in os.listdir(INPUT_DIR) if f.lower().endswith(".pdf")]
    if not pdfs:
        print("No PDF found in data/input_pdf/")
    else:
        extract_all(os.path.join(INPUT_DIR, pdfs[0]))
