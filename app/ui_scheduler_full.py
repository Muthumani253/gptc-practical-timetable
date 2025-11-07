# ui_scheduler_full.py
# Merged 3-page Streamlit UI (fixed): page_upload(), page_scheduler_full(), page_download_full()
# For: MUTHUMANI S — LECTURER-EEE / GPT KARUR

import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import os
from io import BytesIO

# ----- Try imports for existing modules in your app/ directory -----
try:
    import scheduler_logic as sl
    SL_IMPORT_ERROR = None
except Exception as e:
    sl = None
    SL_IMPORT_ERROR = e

try:
    import extract_pdf as extractor
    EXTRACTOR_IMPORT_ERROR = None
except Exception as e:
    extractor = None
    EXTRACTOR_IMPORT_ERROR = e

try:
    from export_word import build_subject_docx_bytes, try_convert_docx_to_pdf
    EXPORT_IMPORT_ERROR = None
except Exception as e:
    build_subject_docx_bytes = None
    try_convert_docx_to_pdf = None
    EXPORT_IMPORT_ERROR = e

# Supabase helpers expected in app/supabase_utils.py
try:
    from supabase_utils import find_or_create_institution, upload_file_bytes
    SUPABASE_UTILS_ERROR = None
except Exception as e:
    find_or_create_institution = None
    upload_file_bytes = None
    SUPABASE_UTILS_ERROR = e

# ---------- App constants & top-level styles ----------
CREATOR = "MUTHUMANI S — LECTURER-EEE / GPT KARUR"
CONTACT = "9443100811"
APP_TITLE = "Practical Timetable Scheduler — Upload · Assign · Export"

st.set_page_config(page_title=APP_TITLE, layout="wide")
st.markdown(f"""
<style>
html, body, [data-testid="stAppViewContainer"] {{
  background: linear-gradient(180deg, #fbfdff 0%, #f3f7fb 50%, #eef2f7 100%);
}}
.banner {{
  background: linear-gradient(90deg, #0ea5a4 0%, #6366f1 100%);
  color: white; padding: 14px 18px; border-radius: 12px; margin-bottom: 14px;
  box-shadow: 0 6px 18px rgba(12, 74, 110, 0.08);
}}
.card {{
  background:#ffffff; border-radius:12px; padding:14px; margin-bottom:12px;
  box-shadow: 0 4px 10px rgba(12,30,60,0.04);
}}
.small-muted {{ color:#6b7280; font-size:13px; }}
.badge {{
  display:inline-block;
  background: rgba(255,255,255,0.12);
  color: rgba(255,255,255,0.95);
  padding:6px 10px; border-radius:999px; margin-left:8px; font-size:13px;
}}
[data-testid="stDateInput"] input {{
    background-color: #ffffff !important;
    color: #111827 !important;
    border: 1px solid #e6edf3 !important;
    border-radius: 8px !important;
}}
.stTextInput > div > div > input,
.stSelectbox [data-baseweb="select"] > div,
.stMultiSelect [data-baseweb="select"] > div {{
    background-color: #ffffff !important;
    color: #111827 !important;
    border-radius: 8px !important;
    border: 1px solid #e6edf3 !important;
}}
[data-baseweb="tag"] {{
    background-color: #ffffff !important;
    color: #111827 !important;
    border: 1px solid #dde6f2 !important;
}}
[data-testid="stExpander"] {{
    background-color: #ffffff !important;
    border-radius: 10px !important;
    border: 1px solid #e9f0fb !important;
    color: #111827 !important;
}}
.conflict-block {{
    margin: 12px auto; max-width: 900px; padding: 12px;
    background:#fff7f6; border:1px solid #fecaca; border-radius:8px;
}}
.conflict-row {{
    background:#fff1f0; border-radius:6px; padding:8px; margin:6px 0;
}}
.center-msg {{
    margin: 12px auto; max-width: 900px; padding: 10px; border-radius: 8px; text-align: left;
}}
.batch-card {{
    padding: 12px; border-radius: 10px; margin-bottom: 12px; border: 1px solid #eef4fb;
}}
.ribbon {{
    display:inline-block; background:#06b6d4; color:white; padding:6px 10px; border-radius:8px; margin-right:10px;
}}
.pill {{
    background:#f1f5ff; padding:6px 10px; border-radius:8px; margin-right:6px; font-size:13px;
}}
.sep {{ height:10px; }}
.metric {{ font-weight:600; color:#0f172a; }}
.progress-wrap {{ background:#eef2ff; border-radius:999px; padding:6px; }}
.progress-fill {{ background:linear-gradient(90deg,#06b6d4,#6366f1); height:10px; border-radius:999px; }}
</style>
""", unsafe_allow_html=True)

st.markdown(f'<div class="banner"><h1 style="margin:0;padding:0">{APP_TITLE}</h1>'
            f'<div style="font-size:13px;margin-top:6px">{CREATOR} <span class="badge">📞 {CONTACT}</span></div></div>',
            unsafe_allow_html=True)

# ---------- Ensure data directories exist (match scheduler_logic paths) ----------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
INPUT_DIR = os.path.join(DATA_DIR, "input_pdf")
EXTRACTED_DIR = os.path.join(DATA_DIR, "extracted")
VERIFIED_DIR = os.path.join(DATA_DIR, "verified")
SCHEDULES_DIR = os.path.join(DATA_DIR, "schedules")
os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(EXTRACTED_DIR, exist_ok=True)
os.makedirs(VERIFIED_DIR, exist_ok=True)
os.makedirs(SCHEDULES_DIR, exist_ok=True)

# ---------- Utility helpers ----------
PALETTE = ["#FDF2F8", "#ECFEFF", "#F0FDF4", "#EEF2FF", "#FFF7ED", "#EFF6FF", "#FFF1F2", "#F5F3FF"]
def bg_for_batch_no(n: int) -> str:
    if n <= 0: return "#ffffff"
    return PALETTE[(n - 1) % len(PALETTE)]

def to_ampm(hhmm_24: str):
    try:
        t = datetime.strptime(hhmm_24, "%H:%M")
        return (t.strftime("%I"), t.strftime("%M"), t.strftime("%p"))
    except Exception:
        return ("09", "00", "AM")

def from_ampm(hour12: str, minute: str, period: str):
    try:
        t = datetime.strptime(f"{hour12}:{minute} {period}", "%I:%M %p")
        return t.strftime("%H:%M")
    except Exception:
        return "09:00"

def to_ddmmyyyy(d: date):
    return d.strftime("%d.%m.%Y")

def parse_ddmmyyyy(s: str) -> date:
    try:
        return datetime.strptime(s, "%d.%m.%Y").date()
    except Exception:
        return date.today()

def fmt_ampm(hhmm: str) -> str:
    try:
        t = datetime.strptime(hhmm, "%H:%M")
        return t.strftime("%I:%M %p").lstrip("0")
    except Exception:
        return hhmm

# ---------- Top navigation (page selection) ----------
nav = st.radio("Go to", ["1 · PDF Upload/Extract (rare)", "2 · Scheduler — Assign Batches", "3 · Download — Finalised"], horizontal=True)

# ---------- Page 1: Upload PDF & Extract (wrapped in function) ----------
def page_upload():
    """
    Single-institution simplified upload/extract page.
    - Upload PDF (saved to data/input_pdf/)
    - Run extractor.extract_all on the saved file
    - Show resulting CSVs (PracticalMaster.csv, StudentSubjectMap.csv) if present
    - Keeps same visual card/banner look as original
    """
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<h2>📤 Page 1 — Upload PDF & Extract CSVs</h2>', unsafe_allow_html=True)
    st.write("Upload a single DOTE Practical Checklist PDF. The app will extract two CSVs and store them locally in `data/extracted/`.")
    st.write("Creator:", CREATOR, "• For queries:", CONTACT)
    st.markdown('</div>', unsafe_allow_html=True)

    # show helpful import errors (but do NOT stop — user may still want to upload and see extractor error)
    if EXTRACTOR_IMPORT_ERROR:
        st.warning(f"Warning: extract_pdf import had an error: {EXTRACTOR_IMPORT_ERROR}. Extraction will fail until fixed.")
    if SUPABASE_UTILS_ERROR:
        # keep as non-fatal info (we're not using supabase in this page)
        st.info("Supabase helpers not available (this app is running in single-institution local mode).")

    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("Upload PDF for extraction")
    pdf_file = st.file_uploader("Select DOTE Practical Checklist PDF", type=["pdf"])
    if pdf_file is not None:
        try:
            pdf_bytes = pdf_file.read()
            # ensure input directory exists
            os.makedirs(INPUT_DIR, exist_ok=True)
            temp_path = os.path.join(INPUT_DIR, pdf_file.name)
            with open(temp_path, "wb") as f:
                f.write(pdf_bytes)
            st.success(f"✅ Saved PDF locally: {temp_path}")

            # run extractor
            if extractor is None:
                st.error("extract_pdf module not available — cannot extract. Check EXTRACTOR_IMPORT_ERROR above.")
            else:
                try:
                    extractor.extract_all(temp_path)
                    st.success("✅ Extraction complete — CSVs written to data/extracted/")
                except Exception as e:
                    st.error(f"Extraction error: {e}")
        except Exception as e:
            st.error(f"Upload/save failed: {e}")

    # Option: run extractor on first PDF in input folder
    if st.button("Run extractor on first PDF in data/input_pdf"):
        files = [f for f in os.listdir(INPUT_DIR) if f.lower().endswith(".pdf")]
        if not files:
            st.warning("No PDF found in data/input_pdf/")
        else:
            path = os.path.join(INPUT_DIR, files[0])
            st.info(f"Running extractor on: {path}")
            if extractor is None:
                st.error("extract_pdf module not available — cannot extract.")
            else:
                try:
                    extractor.extract_all(path)
                    st.success("✅ Extraction complete.")
                except Exception as e:
                    st.error(f"Extraction error: {e}")

    # After extraction: show extracted CSVs if present
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("Extracted CSVs (preview)")

    pm_path = os.path.join(EXTRACTED_DIR, "PracticalMaster.csv")
    ssm_path = os.path.join(EXTRACTED_DIR, "StudentSubjectMap.csv")

    if os.path.exists(pm_path):
        try:
            pm_df = pd.read_csv(pm_path, encoding="utf-8-sig")
            st.markdown("**PracticalMaster.csv**")
            st.dataframe(pm_df.head(200), use_container_width=True, height=240)
            st.download_button("⬇ Download PracticalMaster.csv", data=open(pm_path, "rb").read(),
                               file_name="PracticalMaster.csv", mime="text/csv")
        except Exception as e:
            st.error(f"Failed to read PracticalMaster.csv: {e}")
    else:
        st.info("PracticalMaster.csv not found in data/extracted/ yet.")

    if os.path.exists(ssm_path):
        try:
            ssm_df = pd.read_csv(ssm_path, encoding="utf-8-sig")
            st.markdown("**StudentSubjectMap.csv**")
            st.dataframe(ssm_df.head(300), use_container_width=True, height=320)
            st.download_button("⬇ Download StudentSubjectMap.csv", data=open(ssm_path, "rb").read(),
                               file_name="StudentSubjectMap.csv", mime="text/csv")
        except Exception as e:
            st.error(f"Failed to read StudentSubjectMap.csv: {e}")
    else:
        st.info("StudentSubjectMap.csv not found in data/extracted/ yet.")

    # Quick helper: show files in input/extracted for debug
    with st.expander("Debug: show files in data/input_pdf and data/extracted"):
        input_files = os.listdir(INPUT_DIR) if os.path.isdir(INPUT_DIR) else []
        extracted_files = os.listdir(EXTRACTED_DIR) if os.path.isdir(EXTRACTED_DIR) else []
        st.markdown(f"**data/input_pdf/** ({len(input_files)}): {input_files}")
        st.markdown(f"**data/extracted/** ({len(extracted_files)}): {extracted_files}")
    st.markdown('</div>', unsafe_allow_html=True)


# ---------- Page 2: Scheduler (Assign Batches) ----------
def page_scheduler_full():
    if sl is None:
        st.error(f"scheduler_logic.py import failed: {SL_IMPORT_ERROR}")
        st.stop()

    sl.init_db()

    if "page_local" not in st.session_state:
        st.session_state.page_local = "select_subject"
    if "selected_subject" not in st.session_state:
        st.session_state.selected_subject = None
    if "staged_batches" not in st.session_state:
        st.session_state.staged_batches = {}

    def practical_stats(practical_code: str):
        all_students = sl.get_students_for_practical(practical_code)
        total = int(all_students.shape[0]) if not all_students.empty else 0
        assigned_set = sl.list_assigned_reg_nos_for_practical(practical_code)
        assigned = len(assigned_set)
        remaining = max(total - assigned, 0)
        assigned_pct = int(round((assigned / total) * 100)) if total > 0 else 0

        batches = sl.get_batches(practical_code).copy()
        if batches.empty:
            batches["member_count"] = []
        else:
            member_counts = []
            for _, r in batches.iterrows():
                mem = sl.list_batch_members(int(r["batch_id"]))
                member_counts.append(int(mem.shape[0]))
            batches["member_count"] = member_counts

        return {
            "total": total,
            "assigned": assigned,
            "remaining": remaining,
            "assigned_pct": assigned_pct,
            "batches": batches
        }

    def page_select_subject():
        st.markdown(
            f'<div class="banner"><h2 style="margin:0;padding:0">🔎 Select Subject (Practical)</h2>'
            f'<div style="font-size:13px;margin-top:6px">{CREATOR} <span class="badge">Step 1 of 2</span></div></div>',
            unsafe_allow_html=True
        )
        st.caption("Use the sidebar filters to narrow subjects, then press **Continue → Manage Batches**.")

        with st.sidebar:
            st.markdown("### Filters")
            dept = st.text_input("Department (NCNO)", "")
            sem  = st.text_input("Semester (e.g., 1,2,3...)", "")
            text = st.text_input("Search subject code/name", "")

        pm = sl.list_practicals_by(dept=dept or None, sem=sem or None, text=text or None)

        st.markdown('<div class="card">', unsafe_allow_html=True)
        if pm.empty:
            st.info("No practicals found. Check filters or data files.")
            st.markdown('</div>', unsafe_allow_html=True)
            return

        show = pm[["practical_code","sub_code","subject_name","dept_name","student_count","total_candidates"]].copy()
        show = show.rename(columns={
            "practical_code":"Practical Code",
            "sub_code":"Sub Code",
            "subject_name":"Subject",
            "dept_name":"Department",
            "student_count":"Students Parsed",
            "total_candidates":"Total Candidates"
        })
        st.dataframe(show, use_container_width=True, height=340)

        practical_list = [f"{row['subject_name']} — {row['practical_code']}" for _, row in pm.iterrows()]
        pick = st.selectbox("Choose a subject", practical_list)
        selected_code = pick.split("—")[-1].strip() if "—" in pick else pick

        c = st.columns([1,2,1])[1]
        with c:
            if st.button("Continue → Manage Batches", use_container_width=True):
                st.session_state.selected_subject = selected_code
                st.session_state.page_local = "manage"
                st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    def page_manage():
        practical_code = st.session_state.selected_subject
        if not practical_code:
            st.session_state.page_local = "select_subject"
            st.rerun()

        pm = sl.list_practicals_by()
        subj_row = pm[pm["practical_code"] == practical_code]
        subj_name = subj_row["subject_name"].iloc[0] if not subj_row.empty else practical_code
        dept_name = subj_row["dept_name"].iloc[0] if not subj_row.empty and "dept_name" in subj_row else ""
        st.markdown(
            f'<div class="banner"><h2 style="margin:0;padding:0">📋 {subj_name} — {practical_code}</h2>'
            f'<div style="font-size:13px;margin-top:6px">{CREATOR} <span class="badge">Step 2 of 2</span></div></div>',
            unsafe_allow_html=True
        )

        bcol1, bcol2 = st.columns([3,1])
        with bcol1:
            st.caption("Create batches (up to 3 per day, 3 hours each). Ensure at least 1-hour gap across subjects for each student.")
        with bcol2:
            if st.button("← Back to Subjects", type="secondary", use_container_width=True):
                st.session_state.page_local = "select_subject"
                st.rerun()

        stats = practical_stats(practical_code)
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.subheader("Overview")
        m1, m2, m3 = st.columns(3)
        m1.markdown(f"<div class='metric'>{stats['total']}</div><div class='small-muted'>Total Students</div>", unsafe_allow_html=True)
        m2.markdown(f"<div class='metric'>{stats['assigned']}</div><div class='small-muted'>Assigned</div>", unsafe_allow_html=True)
        m3.markdown(f"<div class='metric'>{stats['remaining']}</div><div class='small-muted'>Remaining</div>", unsafe_allow_html=True)
        st.markdown('<div style="margin-top:8px" class="progress-wrap"><div style="width: {pct}%; " class="progress-fill"></div></div>'.format(pct=stats['assigned_pct']), unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

        bs = stats["batches"].copy()
        st.markdown("#### Batches Summary")
        if bs.empty:
            st.info("No batches yet.")
        else:
            mini = bs[["batch_no","date","start_time","end_time","member_count"]].copy()
            mini["start_time"] = mini["start_time"].apply(fmt_ampm)
            mini["end_time"]   = mini["end_time"].apply(fmt_ampm)
            mini = mini.rename(columns={"batch_no":"Batch","date":"Date","start_time":"Start","end_time":"End","member_count":"Count"})
            st.dataframe(mini, use_container_width=True, height=200)
        st.markdown('</div>', unsafe_allow_html=True)

        # Create Batch Section
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.subheader("Create Batch")
        c1, c2, c3 = st.columns([2,3,2])
        with c1:
            pick_date = st.date_input("Exam Date", value=date.today(), format="DD.MM.YYYY")
            new_date = to_ddmmyyyy(pick_date)
        with c2:
            suggested = sl.suggest_next_start_time(practical_code, new_date)
            hh, mm, ap = to_ampm(suggested)
            col1, col2, col3 = st.columns(3)
            with col1:
                new_hh = st.selectbox("Hour", [f"{i:02d}" for i in range(1,13)],
                                      index=[f"{i:02d}" for i in range(1,13)].index(hh))
            with col2:
                new_mm = st.selectbox("Minute", ["00","15","30","45"],
                                      index=["00","15","30","45"].index(mm if mm in ["00","15","30","45"] else "00"))
            with col3:
                new_ap = st.selectbox("AM/PM", ["AM","PM"], index=0 if ap=="AM" else 1)
            st.caption(f"Suggested next: **{hh}:{mm} {ap}** on {new_date}")
        with c3:
            room = st.text_input("Room/Lab", "")
        if st.button("➕ Create Batch", use_container_width=True):
            start24 = from_ampm(new_hh, new_mm, new_ap)
            ok, res = sl.add_batch_autosequence(practical_code, new_date, start24, room)
            if ok:
                st.success("Batch created.")
                st.rerun()
            else:
                st.error(res)
        st.markdown('</div>', unsafe_allow_html=True)

        sl.reorder_batches(practical_code)
        batches = sl.get_batches(practical_code)
        st.subheader("Batches")
        if batches.empty:
            st.info("No batches yet.")
            return

        try:
            pm_lookup = pm.set_index("practical_code")
        except Exception:
            pm_lookup = pd.DataFrame().set_index([])

        for _, b in batches.sort_values(["date","start_time"]).iterrows():
            batch_id = int(b["batch_id"])
            batch_no = int(b["batch_no"])
            bg = bg_for_batch_no(batch_no)
            disp_start = fmt_ampm(b["start_time"])
            disp_end = fmt_ampm(b["end_time"])
            member_count = sl.list_batch_members(batch_id).shape[0]

            st.markdown(f'<div class="batch-card" style="background:{bg};">', unsafe_allow_html=True)
            st.markdown(
                f"<div class='ribbon'>Batch {batch_no}</div>"
                f"<strong>{b['date']}</strong> &nbsp;&nbsp; "
                f"<span class='pill'>{disp_start} → {disp_end}</span> &nbsp; "
                f"<span class='pill'>Room: {(b.get('room_lab') or '')}</span> &nbsp; "
                f"<span class='pill'>Finalised Students: {member_count}</span>",
                unsafe_allow_html=True
            )

            e1, e2, e3, e4 = st.columns([2,3,2,1])
            with e1:
                cur_date = parse_ddmmyyyy(b["date"])
                new_date_in = st.date_input(f"Date #{batch_no}", value=cur_date, key=f"date_{batch_id}", format="DD.MM.YYYY")
            with e2:
                sh, sm, sap = to_ampm(b["start_time"])
                s1, s2, s3 = st.columns(3)
                with s1:
                    eh = st.selectbox(f"Hour #{batch_no}", [f"{i:02d}" for i in range(1,13)],
                                      index=[f"{i:02d}" for i in range(1,13)].index(sh), key=f"h_{batch_id}")
                with s2:
                    em = st.selectbox(f"Min #{batch_no}", ["00","15","30","45"],
                                      index=["00","15","30","45"].index(sm if sm in ["00","15","30","45"] else "00"), key=f"m_{batch_id}")
                with s3:
                    eap = st.selectbox(f"AM/PM #{batch_no}", ["AM","PM"], index=0 if sap=="AM" else 1, key=f"ap_{batch_id}")
            with e3:
                eroom = st.text_input(f"Room #{batch_no}", value=b.get("room_lab",""), key=f"room_{batch_id}")

            with e4:
                if st.button("💾 Save", key=f"save_{batch_id}", use_container_width=True):
                    nd = to_ddmmyyyy(new_date_in)
                    ns = from_ampm(eh, em, eap)
                    fmt = "%H:%M"
                    try:
                        sdt = datetime.strptime(ns, fmt)
                        edt = sdt + timedelta(minutes=sl.RULES.get("batch_duration_minutes", 180))
                        ne = edt.strftime("%H:%M")
                    except Exception:
                        st.error("Invalid time format.")
                        ne = b["end_time"]

                    mem_df = sl.list_batch_members(batch_id)
                    existing_reg = mem_df["reg_no"].astype(str).tolist() if not mem_df.empty else []
                    staged_now = st.session_state.staged_batches.get(str(batch_id), [])
                    seen = set(); all_check = []
                    for rn in (existing_reg + staged_now):
                        if rn not in seen:
                            seen.add(rn)
                            all_check.append(rn)
                    if all_check:
                        raw_conflicts = sl.check_conflicts_for_students(nd, ns, ne, all_check, exclude_source_batch_id=batch_id)
                    else:
                        raw_conflicts = {}

                    filtered_conflicts = {}
                    for reg, hits in raw_conflicts.items():
                        useful_hits = []
                        for pc, s, e in hits:
                            is_self = False
                            try:
                                if pc == practical_code and s == b["start_time"] and e == b["end_time"] and b["date"] == nd:
                                    is_self = True
                            except Exception:
                                is_self = False
                            if not is_self:
                                useful_hits.append((pc, s, e))
                        if useful_hits:
                            filtered_conflicts[reg] = useful_hits

                    if filtered_conflicts:
                        st.markdown("<div class='center-msg' style='background:#fff7f6;border:1px solid #fecaca;'>"
                                    "<b style='color:#7f1d1d'>⚠️ Cannot update timing — conflicts detected for batch students. Timing not saved.</b></div>",
                                    unsafe_allow_html=True)
                        for idx, rn in enumerate(all_check, start=1):
                            if rn in filtered_conflicts:
                                hits = filtered_conflicts[rn]
                                formatted_hits = []
                                for pc, s, e in hits:
                                    subj_name = str(pc)
                                    try:
                                        if "subject_name" in pm_lookup.columns and pc in pm_lookup.index:
                                            subj_name = str(pm_lookup.loc[pc, "subject_name"])
                                    except Exception:
                                        subj_name = str(pc)
                                    formatted_hits.append(f"{subj_name} — {pc} ({s}-{e})")
                                hits_str = "; ".join(formatted_hits)
                                st.markdown(
                                    f"<div class='conflict-block'><div class='conflict-row'><b>{idx}.</b> <span style='font-family:monospace'>{rn}</span> — {hits_str}</div></div>",
                                    unsafe_allow_html=True
                                )
                    else:
                        ok, msg = sl.update_batch_times(batch_id, date=nd, start_time=ns, room_lab=eroom)
                        if ok:
                            st.markdown("<div class='center-msg' style='background:#ecfdf5;border:1px solid #bbf7d0;'>✅ <b>Batch timing updated successfully.</b></div>", unsafe_allow_html=True)
                            st.rerun()
                        else:
                            st.error(msg)

                if st.button("🗑️", key=f"del_{batch_id}", help="Delete batch", use_container_width=True):
                    ok, msg = sl.delete_batch(batch_id)
                    if ok:
                        st.success("Deleted.")
                        st.rerun()
                    else:
                        st.error(msg)

            st.markdown('<div class="sep"></div>', unsafe_allow_html=True)

            with st.expander(f"➕ Add Students to Batch {batch_no}"):
                rem_df = sl.get_unassigned_students_for_practical(practical_code)
                st.caption(f"Remaining unassigned: **{len(rem_df)}**")

                if rem_df.empty:
                    st.success("All students assigned for this subject. ✅")
                else:
                    options = rem_df["key"].tolist()
                    sel = st.multiselect(f"Select students (Batch {batch_no})", options, key=f"sel_{batch_id}")

                    conflict_map_key = f"conflicts_map_{batch_id}"
                    conflict_order_key = f"conflicts_order_{batch_id}"
                    staged_key = f"staged_{batch_id}"

                    if conflict_map_key not in st.session_state:
                        st.session_state[conflict_map_key] = {}
                    if conflict_order_key not in st.session_state:
                        st.session_state[conflict_order_key] = []
                    if staged_key not in st.session_state:
                        st.session_state[staged_key] = []

                    if st.button("Add to List", key=f"stage_{batch_id}", use_container_width=True):
                        st.markdown("<div class='center-msg' style='background:#eff6ff;border:1px solid #dbeafe;'>Added to staged list (not final).</div>", unsafe_allow_html=True)

                        reg_nos = [s.split(" - ")[0] for s in sel]
                        b_date = b["date"]
                        b_start = b["start_time"]
                        b_end = b["end_time"]

                        conflicts = sl.check_conflicts_for_students(b_date, b_start, b_end, reg_nos)
                        st.session_state[conflict_map_key] = conflicts.copy() if isinstance(conflicts, dict) else {}
                        ordered_conflicts = [r for r in reg_nos if r in st.session_state[conflict_map_key]]
                        st.session_state[conflict_order_key] = ordered_conflicts

                        if conflicts:
                            non_conflicted = [r for r in reg_nos if r not in conflicts]
                            staged_now = st.session_state.get(staged_key, [])
                            staged_now = list(dict.fromkeys(staged_now + non_conflicted))
                            st.session_state[staged_key] = staged_now
                            st.error("⚠️ Conflict(s) detected. Conflicted students were NOT staged.")
                        else:
                            staged_now = st.session_state.get(staged_key, [])
                            staged_now = list(dict.fromkeys(staged_now + reg_nos))
                            st.session_state[staged_key] = staged_now
                            st.session_state[conflict_map_key] = {}
                            st.session_state[conflict_order_key] = []
                            st.success(f"Staged {len(reg_nos)} student(s). Remember to Finalise.")

                    if st.session_state.get(staged_key):
                        st.markdown("**Currently staged (not saved):**")
                        for idx, rn in enumerate(st.session_state[staged_key], start=1):
                            st.markdown(f"{idx}. `{rn}`")

                    if st.button("Save to Batch", key=f"finalise_{batch_id}", use_container_width=True):
                        st.markdown("<div class='center-msg' style='background:#eff6ff;border:1px solid #dbeafe;'>Saving staged students to batch (final).</div>", unsafe_allow_html=True)

                        staged_now = st.session_state.get(staged_key, [])
                        if not staged_now:
                            st.info("No students added in list to finalise.")
                        else:
                            conflicts_before = sl.check_conflicts_for_students(b["date"], b["start_time"], b["end_time"], staged_now)
                            if conflicts_before:
                                st.markdown("<div class='center-msg' style='background:#fff7f6;border:1px solid #fecaca;'>"
                                            "<b style='color:#7f1d1d'>Conflicts detected — finalise blocked.</b></div>", unsafe_allow_html=True)
                                for i, rn in enumerate(staged_now, start=1):
                                    if rn in conflicts_before:
                                        hits = conflicts_before[rn]
                                        formatted_hits = []
                                        for pc, s, e in hits:
                                            subj_name = str(pc)
                                            try:
                                                if "subject_name" in pm_lookup.columns and pc in pm_lookup.index:
                                                    subj_name = str(pm_lookup.loc[pc, "subject_name"])
                                            except Exception:
                                                subj_name = str(pc)
                                            formatted_hits.append(f"{subj_name} — {pc} ({s}-{e})")
                                        hits_str = "; ".join(formatted_hits)
                                        st.markdown(f"<div class='conflict-block'><div class='conflict-row'><b>{i}.</b> <span style='font-family:monospace'>{rn}</span> — {hits_str}</div></div>", unsafe_allow_html=True)
                            else:
                                ok, msg, post_conflicts = sl.add_students_to_batch(batch_id, practical_code, staged_now)
                                if ok:
                                    st.session_state[staged_key] = []
                                    st.session_state[f"conflicts_map_{batch_id}"] = {}
                                    st.session_state[f"conflicts_order_{batch_id}"] = []
                                    st.success(msg)
                                    st.rerun()
                                else:
                                    st.error(msg)
                                    if post_conflicts:
                                        st.markdown("<div class='center-msg' style='background:#fff7f6;border:1px solid #fecaca;'>"
                                                    "<b style='color:#7f1d1d'>Conflicts detected during save.</b></div>", unsafe_allow_html=True)
                                        for i, rn in enumerate(staged_now, start=1):
                                            if rn in post_conflicts:
                                                hits = post_conflicts[rn]
                                                formatted_hits = []
                                                for pc, s, e in hits:
                                                    subj_name = str(pc)
                                                    try:
                                                        if "subject_name" in pm_lookup.columns and pc in pm_lookup.index:
                                                            subj_name = str(pm_lookup.loc[pc, "subject_name"])
                                                    except Exception:
                                                        subj_name = str(pc)
                                                    formatted_hits.append(f"{subj_name} — {pc} ({s}-{e})")
                                                hits_str = "; ".join(formatted_hits)
                                                st.markdown(f"<div class='conflict-block'><div class='conflict-row'><b>{i}.</b> <span style='font-family:monospace'>{rn}</span> — {hits_str}</div></div>", unsafe_allow_html=True)

                    if st.session_state.get(conflict_order_key):
                        st.markdown("<div class='conflict-block'><b style='color:#7f1d1d'>⚠️ Conflicted Students</b></div>", unsafe_allow_html=True)
                        for idx, reg in enumerate(st.session_state[conflict_order_key], start=1):
                            hits = st.session_state[conflict_map_key].get(reg, [])
                            formatted_hits = []
                            for pc, s, e in hits:
                                subj_name = str(pc)
                                try:
                                    if "subject_name" in pm_lookup.columns and pc in pm_lookup.index:
                                        subj_name = str(pm_lookup.loc[pc, "subject_name"])
                                except Exception:
                                    subj_name = str(pc)
                                formatted_hits.append(f"{subj_name} — {pc} ({s}-{e})")
                            hits_str = "; ".join(formatted_hits) if formatted_hits else ""
                            st.markdown(f"<div class='conflict-block'><div class='conflict-row'><b>{idx}.</b> <span style='font-family:monospace'>{reg}</span> — {hits_str}</div></div>", unsafe_allow_html=True)

            with st.expander(f"👥 Batch {batch_no} Members"):
                mem_df = sl.list_batch_members(batch_id, detailed=True)
                if mem_df.empty:
                    st.info("No students in this batch yet.")
                else:
                    mem_df = mem_df.reset_index(drop=True)
                    mem_df.insert(0, "SI.No", mem_df.index + 1)
                    mem_df = mem_df.rename(columns={"reg_no":"Reg No","student_name":"Name","dept_name":"Department"})
                    st.dataframe(mem_df[["SI.No","Reg No","Name","Department"]], use_container_width=True, height=260)
                    cols = st.columns(4)
                    for i, (_, r) in enumerate(mem_df.iterrows()):
                        btn_label = f"remove:{r['Reg No']}"
                        if cols[i % 4].button(btn_label, key=f"rm_{batch_id}_{r['Reg No']}", use_container_width=True):
                            sl.remove_student_from_batch(batch_id, r["Reg No"])
                            st.success(f"Removed {r['Reg No']}.")
                            st.rerun()

            st.markdown('</div>', unsafe_allow_html=True)

        st.markdown('<div class="card">', unsafe_allow_html=True)
        if st.button("💾 Export backup Excel (Batches + BatchMembers)"):
            path = sl.export_backup_excel()
            st.success(f"Saved backup: {path}")
        st.markdown('</div>', unsafe_allow_html=True)

    if st.session_state.page_local == "select_subject":
        page_select_subject()
    else:
        page_manage()

# ---------- Page 3: Download / Export Word & PDF (Header edits removed) ----------
def page_download_full():
    if sl is None:
        st.error(f"scheduler_logic.py import failed: {SL_IMPORT_ERROR}")
        st.stop()
    if build_subject_docx_bytes is None:
        st.error(f"export_word.build_subject_docx_bytes not importable: {EXPORT_IMPORT_ERROR if 'EXPORT_IMPORT_ERROR' in globals() else 'unknown'}")
        st.stop()

    sl.init_db()
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="banner"><h2 style="margin:0;padding:0">📥 Finalised Practicals — Download</h2></div>', unsafe_allow_html=True)
    st.write(f"Creator: **{CREATOR}** • For queries: {CONTACT}")
    st.markdown('</div>', unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("### Filters")
        dept = st.text_input("Department (NCNO)", "")
        sem = st.text_input("Semester", "")
        text = st.text_input("Search subject/code", "")

    pm = sl.list_practicals_by(dept=dept or None, sem=sem or None, text=text or None)
    if pm.empty:
        st.info("No practicals found with current filters.")
        return

    # Build summary: finalised vs not finalised
    finalised = []
    not_finalised = []
    rows = []
    for _, r in pm.iterrows():
        pcode = r["practical_code"]
        total_candidates = int(r.get("total_candidates", 0) or 0)
        assigned_set = sl.list_assigned_reg_nos_for_practical(pcode)
        assigned = len(assigned_set)
        batches = sl.get_batches(pcode)
        is_final = (assigned >= total_candidates and not batches.empty and total_candidates > 0)
        if is_final:
            finalised.append(pcode)
        else:
            not_finalised.append(pcode)
        rows.append({
            "practical_code": pcode,
            "sub_code": r.get("sub_code",""),
            "subject_name": r.get("subject_name",""),
            "dept": r.get("dept_name",""),
            "total": total_candidates,
            "assigned": assigned,
            "batches": int(batches.shape[0])
        })

    summary_df = pd.DataFrame(rows)

    c1, c2 = st.columns([1,1])
    with c1:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.subheader(f"✅ Completed / Finalised ({len(finalised)})")
        if finalised:
            fin_df = summary_df[summary_df["practical_code"].isin(finalised)][["sub_code","subject_name","practical_code","total","assigned","batches"]]
            fin_df = fin_df.rename(columns={"sub_code":"Sub Code","subject_name":"Subject","practical_code":"PCode","total":"Total","assigned":"Assigned","batches":"Batches"})
            st.dataframe(fin_df, use_container_width=True, height=260)
            st.markdown("**Downloads** — click per-row button to download Word (.docx) for that practical.")
            for _, fr in fin_df.iterrows():
                pcode = fr["PCode"]
                label = f"{fr['Sub Code']} — {fr['Subject']}  ({fr['Assigned']}/{fr['Total']})"
                cols = st.columns([4,1])
                cols[0].markdown(f"**{label}**")
                fname_base = f"{fr['Sub Code']}_{fr['Subject']}".replace(" ", "_")
                if cols[1].button(f"📝 Download", key=f"dl_{pcode}"):
                    try:
                        with st.spinner(f"Generating {fname_base}..."):
                            docx = build_subject_docx_bytes(pcode, header_overrides=None)
                        st.success(f"Prepared {fname_base}_Timetable.docx — click Download below")
                        st.download_button(label=f"⬇ Download {fname_base}.docx", data=docx,
                                           file_name=f"{fname_base}_Timetable.docx",
                                           mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                                           key=f"dlbtn_{pcode}")
                    except Exception as e:
                        st.error(f"Error generating {pcode}: {e}")
        else:
            st.info("None yet — complete assignment & create batches to finalise.")
        st.markdown('</div>', unsafe_allow_html=True)

    with c2:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.subheader(f"⚠️ Not finalised ({len(not_finalised)})")
        if not_finalised:
            not_df = summary_df[~summary_df["practical_code"].isin(finalised)][["sub_code","subject_name","practical_code","total","assigned","batches"]]
            not_df = not_df.rename(columns={"sub_code":"Sub Code","subject_name":"Subject","practical_code":"PCode","total":"Total","assigned":"Assigned","batches":"Batches"})
            st.dataframe(not_df, use_container_width=True, height=360)
        else:
            st.success("All practicals finalised.")
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("---")
    st.caption("Filename format: <SubCode>_<SubjectName>_Timetable.docx — Created by MUTHUMANI S / LECTURER-EEE / GPT-KARUR • For queries: 9443100811")

# ---------- Router ----------
if nav.startswith("1"):
    page_upload()
elif nav.startswith("2"):
    page_scheduler_full()
elif nav.startswith("3"):
    page_download_full()
else:
    st.info("Choose a page from the navigation above.")
