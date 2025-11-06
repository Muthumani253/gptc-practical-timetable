#!/usr/bin/env python3
# extract_pdf.py  — dept mapping embedded in this file (no JSON)
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

# Ensure stdout uses utf-8 on Windows (avoid UnicodeEncodeError in prints)
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

# --- Setup paths ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
INPUT_DIR = os.path.join(DATA_DIR, "input_pdf")
EXTRACTED_DIR = os.path.join(DATA_DIR, "extracted")

os.makedirs(EXTRACTED_DIR, exist_ok=True)
os.makedirs(INPUT_DIR, exist_ok=True)

# ---------------------------
# EMBEDDED DEPARTMENT MAP
# ---------------------------
# Map NCNO (dept code) -> human-friendly dept name.
# Edit/add codes here as needed.
department_map = {
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
# --- Helper functions ---

def parse_institution(text):
    """
    Extract simple institution info from first page.
    Returns (ins_code, institute_line, header_text)
    """
    ins_code = ""
    institute_line = ""
    header_text = ""

    m = re.search(r"Ins\s*Code\s*Name\s*of\s*the\s*Institution\s*\n+(\d{2,4})\s+([^\n]+)", text, flags=re.IGNORECASE)
    if m:
        ins_code = m.group(1).strip()
        institute_line = m.group(2).strip()

    if not ins_code:
        m = re.search(r"\bIns(?:titution)?\s*Code.*?\n+(\d{2,4})\b", text, flags=re.IGNORECASE)
        if m:
            ins_code = m.group(1).strip()

    if not institute_line:
        m2 = re.search(r"(\d{2,4}\s*,?\s*GOVERNMENT\s+POLYTECHNIC\s+COLLEGE[^\n]*)", text, flags=re.IGNORECASE)
        if m2:
            institute_line = m2.group(1).strip()
            if not ins_code:
                mcode = re.search(r"^\s*(\d{2,4})\b", institute_line)
                if mcode:
                    ins_code = mcode.group(1).strip()

    if ins_code and institute_line:
        header_text = institute_line if institute_line.strip().startswith(ins_code) else f"{ins_code} , {institute_line}"
    elif ins_code:
        header_text = ins_code

    return ins_code or "", institute_line or "", header_text or ""

def detect_page_kind(text):
    if re.search(r"PRACTICAL\s+CHECK\s+LIST\s*\(SUMMARY\)", text, flags=re.IGNORECASE):
        return "summary"
    if re.search(r"PRACTICAL\s+CHECK\s+LIST\s*::", text, flags=re.IGNORECASE):
        return "subject"
    return None

def extract_summary_rows(text):
    rows = []
    started = False
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        if re.search(r"\bSNo\b.*\bNCNO\b.*\bSubCode\b.*\bType\b.*\bNoC\b", line, flags=re.IGNORECASE):
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
            maybe_type = parts[-2].strip()
            noc = parts[-1].strip()
            subject_name = " ".join(parts[3:-2]).strip()

            if not re.fullmatch(r"\d+", s_no):
                continue
            if not re.fullmatch(r"\d{3,4}", ncno):
                continue
            if not re.fullmatch(r"[A-Z0-9\-]+", sub_code):
                continue
            if not re.fullmatch(r"(P|PT|ASC)", maybe_type):
                continue
            if not re.fullmatch(r"\d+", noc):
                continue

            rows.append({
                "s_no": int(s_no),
                "ncno": ncno,
                "sub_code": sub_code,
                "subject_name": subject_name,
                "type": maybe_type,
                "noc": int(noc),
            })
        except Exception:
            continue
    return rows

def extract_subject_header(text):
    practical_code = None
    subject_name = None
    ptype = None
    for line in text.splitlines():
        line = line.strip()
        m = re.match(r"(\d{2,4}-\d{3,4}-[A-Z0-9\-]+)\s+(.+?)\s+(P|PT|ASC)\s*$", line)
        if m:
            practical_code = m.group(1)
            subject_name = m.group(2).strip()
            ptype = m.group(3)
            break
    return practical_code, subject_name, ptype

def extract_student_rows(text):
    rows = []
    started = False
    header_seen = False

    header_pattern = re.compile(r"S\.?No\s+NCNO\s+Reg\s*No\s+Name.*DoB\s+Regl\s+Sem\s+Col", re.IGNORECASE)
    line_pattern = re.compile(
        r"^\s*(\d+)\s+(\d{3,4})\s+(\d+)\s+(.+?)\s+(\d{2}\.\d{2}\.\d{4})\s+([A-Z0-9]+)\s+(\d+)\s+(\d+)\s*$"
    )

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            if started:
                break
            continue

        if line.startswith("Page No:"):
            continue

        if not header_seen and header_pattern.search(line):
            header_seen = True
            started = True
            continue

        if started:
            m = line_pattern.match(line)
            if m:
                try:
                    s_no = int(m.group(1))
                    ncno = m.group(2)
                    reg_no = m.group(3)
                    name = m.group(4).strip()
                    dob = m.group(5)
                    regl = m.group(6)
                    sem = int(m.group(7))
                    col_no = int(m.group(8))

                    rows.append({
                        "s_no": s_no,
                        "ncno": ncno,
                        "reg_no": reg_no,
                        "student_name": name,
                        "dob": dob,
                        "regl": regl,
                        "sem": sem,
                        "col_no": col_no,
                    })
                except Exception:
                    pass
            else:
                parts = re.split(r"\s{1,}", line)
                if len(parts) >= 8:
                    try:
                        s_no = int(parts[0])
                        ncno = parts[1]
                        reg_no = parts[2]
                        dob = parts[-4]
                        regl = parts[-3]
                        sem = int(parts[-2])
                        col_no = int(parts[-1])
                        name = " ".join(parts[3:-4]).strip()
                        rows.append({
                            "s_no": s_no,
                            "ncno": ncno,
                            "reg_no": reg_no,
                            "student_name": name,
                            "dob": dob,
                            "regl": regl,
                            "sem": sem,
                            "col_no": col_no,
                        })
                    except Exception:
                        pass

    return rows

def month_year_from_text(text):
    m = re.search(r"(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s+(\d{4})",
                  text, flags=re.IGNORECASE)
    if m:
        return f"{m.group(1)[:3].upper()} {m.group(2)}"
    return datetime.now().strftime("%b %Y").upper()

# --- Main extraction logic ---

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
                ins_code, institute_line, header_text = parse_institution(text)
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
                    dept_name = DEPT_MAP.get(str(ncno).strip(), "")
                    practical_master[practical_code] = {
                        "ins_code": ins_code or "",
                        "ncno": ncno,
                        "dept_name": dept_name,
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
                    continue

                # If dept name not in summary, try to infer from this page header lines
                # Best effort: search for lines that include NCNO-like pattern near top
                page_lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
                page_top = " ".join(page_lines[:6]) if page_lines else ""
                # find first ncno-like token on page
                found_ncno = None
                mnc = re.search(r"\b(\d{3,4})\b", page_top)
                if mnc:
                    found_ncno = mnc.group(1)

                stud_rows = extract_student_rows(text)
                print(f"  -> Extracting student list ... {len(stud_rows)} found")

                col_no = None
                if stud_rows:
                    col_no = Counter([r["col_no"] for r in stud_rows]).most_common(1)[0][0]

                # Determine dept_name for this practical: priority
                # 1) if NCNO exists in student rows, use DEPT_MAP[ncno]
                # 2) else if found_ncno from page header, use DEPT_MAP[found_ncno]
                # 3) else leave empty (caller may prefer subject fallback)
                practical_ncno = None
                if stud_rows:
                    practical_ncno = str(stud_rows[0]["ncno"]).strip()
                elif found_ncno:
                    practical_ncno = str(found_ncno).strip()

                p_dept_name = DEPT_MAP.get(practical_ncno, "") if practical_ncno else ""

                for s in stud_rows:
                    ncno = s["ncno"]
                    # dept name per student: prefer mapping; if mapping missing, use page-level p_dept_name
                    dept_name = DEPT_MAP.get(str(ncno).strip(), "") or p_dept_name or ""
                    student_rows_all.append({
                        "reg_no": s["reg_no"],
                        "student_name": s["student_name"],
                        "dob": s["dob"],
                        "regl": s["regl"],
                        "sem": s["sem"],
                        "ncno": ncno,
                        "dept_name": dept_name,
                        "sub_code": practical_code.split("-")[-1] if practical_code else "",
                        "subject_name": subject_name_pg or "",
                        "type": ptype_pg or "",
                        "col_no": s["col_no"],
                        "practical_code": practical_code,
                        "ins_code": practical_code.split("-")[0] if practical_code and "-" in practical_code else (ins_code or ""),
                    })

                if practical_code not in practical_master:
                    practical_master[practical_code] = {
                        "ins_code": practical_code.split("-")[0] if practical_code and "-" in practical_code else (ins_code or ""),
                        "ncno": practical_ncno or "",
                        "dept_name": p_dept_name or "",
                        "sub_code": practical_code.split("-")[-1] if practical_code else "",
                        "subject_name": subject_name_pg or "",
                        "type": ptype_pg or "",
                        "col_no": col_no,
                        "total_candidates": len(stud_rows),
                        "practical_code": practical_code,
                        "exam_month_year": exam_month_year or "",
                    }
                else:
                    if practical_master[practical_code]["col_no"] is None and col_no is not None:
                        practical_master[practical_code]["col_no"] = col_no
                    if stud_rows:
                        practical_master[practical_code]["total_candidates"] = max(
                            practical_master[practical_code]["total_candidates"], len(stud_rows)
                        )
            else:
                print("Skipped (no match)")

    # Paths
    pm_path = os.path.join(EXTRACTED_DIR, "PracticalMaster.csv")
    ssm_path = os.path.join(EXTRACTED_DIR, "StudentSubjectMap.csv")

    pm_cols = ["ins_code","ncno","dept_name","sub_code","subject_name","type","col_no","total_candidates","practical_code","exam_month_year"]
    ssm_cols = ["reg_no","student_name","dob","regl","sem","ncno","dept_name","sub_code","subject_name","type","col_no","practical_code","ins_code"]

    pm_df = pd.DataFrame([practical_master[k] for k in sorted(practical_master.keys())], columns=pm_cols)
    ssm_df = pd.DataFrame(student_rows_all, columns=ssm_cols)

    # Write CSVs (utf-8-sig for Excel compatibility)
    pm_df.to_csv(pm_path, index=False, encoding="utf-8-sig")
    ssm_df.to_csv(ssm_path, index=False, encoding="utf-8-sig")

    # Log
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
    print("\nExtraction complete.\n")

def find_default_pdf():
    if not os.path.isdir(INPUT_DIR):
        return None
    pdfs = [os.path.join(INPUT_DIR, x) for x in os.listdir(INPUT_DIR) if x.lower().endswith(".pdf")]
    return pdfs[0] if pdfs else None

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", "-i", help="Path to the DOTE Practical Checklist PDF")
    args = parser.parse_args()

    pdf_path = args.input or find_default_pdf()
    if not pdf_path or not os.path.exists(pdf_path):
        print("ERROR: No input PDF found. Put your file in data/input_pdf/ or pass --input path.")
        sys.exit(1)

    extract_all(pdf_path)

if __name__ == "__main__":
    main()
