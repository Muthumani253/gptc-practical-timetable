#!/usr/bin/env python3
# extract_pdf.py (corrected)
# Produces:
#  - data/extracted/PracticalMaster.csv
#  - data/extracted/StudentSubjectMap.csv

import re
import os
import sys
import json
import argparse
from collections import Counter
from datetime import datetime

import pdfplumber
import pandas as pd

# --- Try to ensure stdout uses utf-8 on Windows (you already added this elsewhere) ---
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

# --- Setup paths ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
INPUT_DIR = os.path.join(DATA_DIR, "input_pdf")
EXTRACTED_DIR = os.path.join(DATA_DIR, "extracted")
SETTINGS_DIR = os.path.join(PROJECT_ROOT, "settings")

os.makedirs(EXTRACTED_DIR, exist_ok=True)
os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(SETTINGS_DIR, exist_ok=True)


# --- Helper functions ---

def load_dept_map():
    path = os.path.join(SETTINGS_DIR, "dept_codes.json")
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def parse_institution(text):
    """
    Extract institution code and name from the first page text.
    Returns: (ins_code, institute_line, header_text)
    header_text is a friendly header string (ins_code + institute_line) when available.
    """
    ins_code = None
    institute_line = None
    header_text = None

    # Pattern A: "Ins Code Name of the Institution" block
    m = re.search(r"Ins\s*Code\s*Name\s*of\s*the\s*Institution\s*\n+(\d{2,4})\s+([^\n]+)", text, flags=re.IGNORECASE)
    if m:
        ins_code = m.group(1).strip()
        institute_line = m.group(2).strip()

    # Pattern B: "Institution Code" wording
    if not ins_code:
        m = re.search(r"\bIns(?:titution)?\s*Code.*?\n+(\d{2,4})\b", text, flags=re.IGNORECASE)
        if m:
            ins_code = m.group(1).strip()

    # Pattern C: Any line starting with "<code> GOVERNMENT POLYTECHNIC COLLEGE ..."
    if not institute_line:
        m2 = re.search(r"(\d{2,4}\s*,?\s*GOVERNMENT\s+POLYTECHNIC\s+COLLEGE[^\n]*)", text, flags=re.IGNORECASE)
        if m2:
            institute_line = m2.group(1).strip()
            if not ins_code:
                mcode = re.search(r"^\s*(\d{2,4})\b", institute_line)
                if mcode:
                    ins_code = mcode.group(1).strip()

    # Compose header text
    if ins_code and institute_line:
        if not institute_line.strip().startswith(ins_code):
            header_text = f"{ins_code} , {institute_line}"
        else:
            header_text = institute_line
    elif ins_code:
        header_text = f"{ins_code}"
    elif institute_line:
        header_text = institute_line

    return ins_code, institute_line, header_text


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


# --- Main Extraction ---
def extract_all(pdf_path):
    dept_map = load_dept_map()
    practical_master = {}
    student_rows_all = []

    ins_code = None
    institute_line = ""
    header_text = ""
    exam_month_year = None

    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        print(f"\n[INFO] Starting extraction from: {os.path.basename(pdf_path)}")
        print(f"[INFO] Total pages detected: {total_pages}\n")

        for pno, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            # safe print; avoid unicode characters in print if console doesn't support
            try:
                print(f"[PAGE {pno}/{total_pages}] Reading... ", end="")
            except Exception:
                pass

            if pno == 1:
                ins_code, institute_line, header_text = parse_institution(text)
                exam_month_year = month_year_from_text(text)

            kind = detect_page_kind(text)
            if kind == "summary":
                try:
                    print("Summary section found")
                except Exception:
                    pass
                rows = extract_summary_rows(text)
                for r in rows:
                    ncno = r["ncno"]
                    sub_code = r["sub_code"]
                    subject_name = r["subject_name"]
                    ptype = r["type"]
                    noc = r["noc"]
                    practical_code = f"{ins_code}-{ncno}-{sub_code}" if ins_code else f"{ncno}-{sub_code}"

                    # dept_name: try mapping, fallback to ncno so CSV isn't blank
                    dept_name = dept_map.get(ncno)
                    if dept_name is None or str(dept_name).strip() == "":
                        dept_name = str(ncno)

                    practical_master[practical_code] = {
                        "ins_code": ins_code or "",
                        "institute_line": header_text or institute_line or "",
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
                try:
                    print(f"Subject section: {practical_code or 'Unknown'}")
                except Exception:
                    pass
                if not practical_code:
                    continue

                try:
                    print(f"  -> Extracting student list for {practical_code} ...", end="")
                except Exception:
                    pass
                stud_rows = extract_student_rows(text)
                try:
                    print(f" {len(stud_rows)} students found")
                except Exception:
                    pass

                col_no = None
                if stud_rows:
                    col_no = Counter([r["col_no"] for r in stud_rows]).most_common(1)[0][0]

                for s in stud_rows:
                    ncno = s["ncno"]
                    dept_name = dept_map.get(ncno)
                    if dept_name is None or str(dept_name).strip() == "":
                        dept_name = str(ncno)

                    student_rows_all.append({
                        "reg_no": s["reg_no"],
                        "student_name": s["student_name"],
                        "dob": s["dob"],
                        "regl": s["regl"],
                        "sem": s["sem"],
                        "ncno": ncno,
                        "dept_name": dept_name,
                        "sub_code": practical_code.split("-")[-1],
                        "subject_name": subject_name_pg or "",
                        "type": ptype_pg or "",
                        "col_no": s["col_no"],
                        "practical_code": practical_code,
                        "ins_code": practical_code.split("-")[0] if "-" in practical_code else (ins_code or ""),
                    })

                if practical_code not in practical_master:
                    ncno = practical_code.split("-")[1] if "-" in practical_code and len(practical_code.split("-")) > 1 else ""
                    dept_name = dept_map.get(ncno)
                    if dept_name is None or str(dept_name).strip() == "":
                        dept_name = str(ncno)
                    practical_master[practical_code] = {
                        "ins_code": practical_code.split("-")[0] if "-" in practical_code else (ins_code or ""),
                        "institute_line": header_text or institute_line or "",
                        "ncno": ncno,
                        "dept_name": dept_name,
                        "sub_code": practical_code.split("-")[-1],
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
                try:
                    print("Skipped (no match)")
                except Exception:
                    pass

    # Write CSVs
    pm_path = os.path.join(EXTRACTED_DIR, "PracticalMaster.csv")
    ssm_path = os.path.join(EXTRACTED_DIR, "StudentSubjectMap.csv")

    pm_cols = ["ins_code", "institute_line", "ncno", "dept_name", "sub_code", "subject_name", "type", "col_no", "total_candidates", "practical_code", "exam_month_year"]
    ssm_cols = ["reg_no", "student_name", "dob", "regl", "sem", "ncno", "dept_name", "sub_code", "subject_name", "type", "col_no", "practical_code", "ins_code"]

    pm_df = pd.DataFrame([practical_master[k] for k in sorted(practical_master.keys())], columns=pm_cols)
    ssm_df = pd.DataFrame(student_rows_all, columns=ssm_cols)

    # Save with utf-8-sig to help Excel/Streamlit
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
    try:
        print("\n✅ Extraction complete.\n")
    except Exception:
        pass


def find_default_pdf():
    if not os.path.isdir(INPUT_DIR):
        return None
    pdfs = [os.path.join(INPUT_DIR, x) for x in os.listdir(INPUT_DIR) if x.lower().endswith(".pdf")]
    return pdfs[0] if pdfs else None


def main():
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
