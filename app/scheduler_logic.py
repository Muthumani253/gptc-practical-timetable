# scheduler_logic.py
# Core data access + scheduling rules for batches and conflict checks
# Restored original logic; improved conflict checking only.

import os
import sqlite3
from datetime import datetime, timedelta
import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
VERIFIED_DIR = os.path.join(DATA_DIR, "verified")
EXTRACTED_DIR = os.path.join(DATA_DIR, "extracted")
SCHEDULES_DIR = os.path.join(DATA_DIR, "schedules")

DB_PATH = os.path.join(SCHEDULES_DIR, "timetable.db")
BACKUP_XLSX = os.path.join(SCHEDULES_DIR, "timetable_backup.xlsx")

RULES = {
    "batch_size_max": 30,
    "batch_duration_minutes": 180,
    "max_batches_per_day_per_practical": 3,
    "min_gap_minutes": 60
}

def _connect():
    os.makedirs(SCHEDULES_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def init_db():
    conn = _connect()
    cur = conn.cursor()

    cur.execute("""CREATE TABLE IF NOT EXISTS Batches (
        batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
        practical_code TEXT NOT NULL,
        batch_no INTEGER NOT NULL,
        day_index INTEGER NOT NULL,
        date TEXT NOT NULL,            -- dd.mm.yyyy
        start_time TEXT NOT NULL,      -- HH:MM 24h
        end_time TEXT NOT NULL,        -- HH:MM 24h
        room_lab TEXT,
        status TEXT DEFAULT 'draft',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );""")

    cur.execute("""CREATE TABLE IF NOT EXISTS BatchMembers (
        batch_member_id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id INTEGER NOT NULL,
        reg_no TEXT NOT NULL,
        practical_code TEXT NOT NULL,
        added_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(batch_id, reg_no),
        FOREIGN KEY(batch_id) REFERENCES Batches(batch_id) ON DELETE CASCADE
    );""")

    cur.execute("""CREATE TABLE IF NOT EXISTS AssignmentsIndex (
        idx_id INTEGER PRIMARY KEY AUTOINCREMENT,
        reg_no TEXT NOT NULL,
        date TEXT NOT NULL,        -- dd.mm.yyyy
        start_time TEXT NOT NULL,
        end_time TEXT NOT NULL,
        practical_code TEXT NOT NULL,
        source_batch_id INTEGER NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );""")

    conn.commit()
    conn.close()

# ---------- CSV loaders ----------

def _load_csv(name):
    v = os.path.join(VERIFIED_DIR, name)
    e = os.path.join(EXTRACTED_DIR, name)
    path = v if os.path.exists(v) else e
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path)

def load_practicals():
    return _load_csv("PracticalMaster_verified.csv") if os.path.exists(
        os.path.join(VERIFIED_DIR, "PracticalMaster_verified.csv")
    ) else _load_csv("PracticalMaster.csv")

def load_students():
    return _load_csv("StudentSubjectMap_verified.csv") if os.path.exists(
        os.path.join(VERIFIED_DIR, "StudentSubjectMap_verified.csv")
    ) else _load_csv("StudentSubjectMap.csv")

# ---------- Practical & student lists ----------

def list_practicals_by(dept=None, sem=None, text=None):
    pm = load_practicals().copy()
    ssm = load_students()[["practical_code", "reg_no"]].drop_duplicates()
    counts = ssm.groupby("practical_code")["reg_no"].nunique().rename("student_count")
    pm = pm.merge(counts, on="practical_code", how="left").fillna({"student_count":0})
    if dept:
        pm = pm[pm["ncno"].astype(str) == str(dept)]
    if sem:
        s = load_students()
        pc_sem = s[s["sem"].astype(str) == str(sem)]["practical_code"].unique()
        pm = pm[pm["practical_code"].isin(pc_sem)]
    if text:
        m = pm["subject_name"].astype(str).str.contains(text, case=False) | pm["sub_code"].astype(str).str.contains(text, case=False)
        pm = pm[m]
    return pm.sort_values(["ncno", "sub_code"])

def get_students_for_practical(practical_code):
    s = load_students()
    subset = s[s["practical_code"] == practical_code].copy()
    subset["reg_no"] = subset["reg_no"].astype(str)
    subset["key"] = subset["reg_no"] + " - " + subset["student_name"].astype(str)
    return subset

def list_assigned_reg_nos_for_practical(practical_code):
    init_db()
    conn = _connect()
    df = pd.read_sql_query(
        "SELECT DISTINCT reg_no FROM BatchMembers WHERE practical_code=?",
        conn, params=[practical_code]
    )
    conn.close()
    return set(df["reg_no"].astype(str).tolist())

def get_unassigned_students_for_practical(practical_code):
    all_df = get_students_for_practical(practical_code)
    assigned = list_assigned_reg_nos_for_practical(practical_code)
    if not assigned:
        return all_df.copy()
    unassigned = all_df[~all_df["reg_no"].astype(str).isin(assigned)].copy()
    return unassigned

# ---------- Time helpers ----------

def _time_add(start_hhmm, minutes):
    hh, mm = map(int, start_hhmm.split(":"))
    dt = datetime(2000,1,1,hh,mm) + timedelta(minutes=minutes)
    return f"{dt.hour:02d}:{dt.minute:02d}"

def _valid_hhmm(s):
    try:
        datetime.strptime(s, "%H:%M")
        return True
    except Exception:
        return False

# ---------- Batches CRUD & renumber ----------

def get_batches(practical_code, date=None):
    conn = _connect()
    q = "SELECT * FROM Batches WHERE practical_code=?"
    params = [practical_code]
    if date:
        q += " AND date=?"
        params.append(date)
    q += " ORDER BY date, time(start_time)"
    df = pd.read_sql_query(q, conn, params=params)
    conn.close()
    return df

def count_batches_on_day(practical_code, date):
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM Batches WHERE practical_code=? AND date=?", (practical_code, date))
    c = cur.fetchone()[0] or 0
    conn.close()
    return c

def total_candidates_for_practical(practical_code):
    pm = load_practicals()
    row = pm[pm["practical_code"] == practical_code]
    return int(row["total_candidates"].iloc[0]) if not row.empty else 0

def ensure_rules_before_batch(practical_code, date, intended_batch_no):
    total = total_candidates_for_practical(practical_code)
    if total <= 90:
        if intended_batch_no > 3:
            return False, "For ≤90 candidates, maximum 3 batches allowed on a single day."
        if count_batches_on_day(practical_code, date) >= RULES["max_batches_per_day_per_practical"]:
            return False, "Already 3 batches on this day."
    if count_batches_on_day(practical_code, date) >= RULES["max_batches_per_day_per_practical"]:
        return False, "Max 3 batches per day for a practical."
    return True, ""

def _distinct_dates_sorted(practical_code):
    conn = _connect()
    df = pd.read_sql_query("SELECT DISTINCT date FROM Batches WHERE practical_code=?", conn, params=[practical_code])
    conn.close()
    if df.empty:
        return []
    return sorted(df["date"].tolist(), key=lambda x: datetime.strptime(x, "%d.%m.%Y"))

def reorder_batches(practical_code):
    """Re-number batch_no = 1..n (single sequence across ALL dates) by (date → start_time),
       and recompute day_index per date order."""
    init_db()
    dates = _distinct_dates_sorted(practical_code)
    if not dates:
        return
    conn = _connect()
    cur = conn.cursor()
    new_no = 1
    for di, dt_str in enumerate(dates, start=1):
        q = """SELECT batch_id FROM Batches
               WHERE practical_code=? AND date=?
               ORDER BY time(start_time)"""
        rows = pd.read_sql_query(q, conn, params=[practical_code, dt_str])
        for _, r in rows.iterrows():
            cur.execute("UPDATE Batches SET batch_no=?, day_index=? WHERE batch_id=?",
                        (new_no, di, int(r["batch_id"])))
            new_no += 1
    conn.commit()
    conn.close()

def suggest_next_start_time(practical_code, date):
    """Return next start time = latest end_time on that date, else 09:00."""
    conn = _connect()
    df = pd.read_sql_query(
        "SELECT end_time FROM Batches WHERE practical_code=? AND date=? ORDER BY time(end_time) DESC",
        conn, params=[practical_code, date]
    )
    conn.close()
    if df.empty:
        return "09:00"
    return df["end_time"].iloc[0]

def add_batch_autosequence(practical_code, date, start_time=None, room_lab=None, status="draft"):
    """Add a batch using given start_time OR auto-pick next 3h slot. Reorders afterward."""
    init_db()
    # number before insertion
    intended_no = get_batches(practical_code, date=date).shape[0] + 1
    ok, msg = ensure_rules_before_batch(practical_code, date, intended_no)
    if not ok:
        return False, msg

    if not start_time or not _valid_hhmm(start_time):
        start_time = suggest_next_start_time(practical_code, date)
    end_time = _time_add(start_time, RULES["batch_duration_minutes"])

    conn = _connect()
    cur = conn.cursor()
    cur.execute("""INSERT INTO Batches(practical_code,batch_no,day_index,date,start_time,end_time,room_lab,status)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (practical_code, intended_no, 1, date, start_time, end_time, room_lab, status))
    conn.commit()
    batch_id = cur.lastrowid
    conn.close()

    reorder_batches(practical_code)
    return True, batch_id

def update_batch_times(batch_id, date=None, start_time=None, room_lab=None):
    """Manually edit date/start; end_time auto = start+180. Then reorder."""
    init_db()
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT practical_code, date, start_time FROM Batches WHERE batch_id=?", (batch_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return False, "Batch not found."
    practical_code, old_date, old_start = row

    new_date = date or old_date
    new_start = start_time or old_start
    if not _valid_hhmm(new_start):
        conn.close()
        return False, "Invalid start time. Use HH:MM (24h)."
    new_end = _time_add(new_start, RULES["batch_duration_minutes"])

    cur.execute("""UPDATE Batches SET date=?, start_time=?, end_time=?, room_lab=COALESCE(?, room_lab),
                   updated_at=CURRENT_TIMESTAMP WHERE batch_id=?""",
                (new_date, new_start, new_end, room_lab, batch_id))
    conn.commit()
    conn.close()

    reorder_batches(practical_code)
    return True, "Batch timing updated."

def delete_batch(batch_id: int):
    """
    Deletes one batch and cleans up:
      - BatchMembers (via ON DELETE CASCADE)
      - AssignmentsIndex (manual delete by source_batch_id)
    Then renumbers the remaining batches 1..n for that practical.
    """
    init_db()
    conn = _connect()
    cur = conn.cursor()

    cur.execute("SELECT practical_code FROM Batches WHERE batch_id=?", (batch_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return False, f"Batch {batch_id} not found."
    practical_code = row[0]

    cur.execute("DELETE FROM AssignmentsIndex WHERE source_batch_id=?", (batch_id,))
    cur.execute("DELETE FROM Batches WHERE batch_id=?", (batch_id,))
    conn.commit()
    conn.close()

    reorder_batches(practical_code)
    return True, f"Batch {batch_id} deleted."

# ---------- Conflicts & membership ----------

def _parse_hhmm(s: str) -> str:
    """Normalize HH:MM string (pad single digits)."""
    s = str(s).strip()
    if ":" in s:
        parts = s.split(":")
        h = parts[0].zfill(2)
        m = parts[1].zfill(2)
        return f"{h}:{m}"
    # fallback: assume already correct
    return s

def _overlap_or_gap_less_than(a_start, a_end, b_start, b_end, min_gap):
    """Return True when intervals overlap OR gap between them is < min_gap (minutes).
       a_start,a_end,b_start,b_end expected as 'HH:MM' strings.
       If gap == min_gap -> allowed (no conflict). Conflict only when gap < min_gap.
    """
    fmt = "%H:%M"
    A_s = datetime.strptime(_parse_hhmm(a_start), fmt)
    A_e = datetime.strptime(_parse_hhmm(a_end), fmt)
    B_s = datetime.strptime(_parse_hhmm(b_start), fmt)
    B_e = datetime.strptime(_parse_hhmm(b_end), fmt)

    # overlap check (strict)
    if A_s < B_e and B_s < A_e:
        return True

    # compute gaps (B_s after A_e or A_s after B_e)
    gap1 = (B_s - A_e).total_seconds() / 60.0  # minutes between A_e and B_s
    gap2 = (A_s - B_e).total_seconds() / 60.0  # minutes between B_e and A_s

    # If either gap is positive and strictly less than min_gap -> conflict.
    if 0 <= gap1 < min_gap or 0 <= gap2 < min_gap:
        return True

    return False

def check_conflicts_for_students(date, start_time, end_time, reg_nos, exclude_source_batch_id=None):
    """Return dict reg_no -> list of (practical_code, start_time, end_time) that conflict.
    Uses AssignmentsIndex (records of already assigned batches).
    If exclude_source_batch_id is provided, rows where source_batch_id == exclude_source_batch_id
    are ignored (useful when verifying timing changes for an existing batch).
    """
    init_db()
    conn = _connect()

    # exclude row(s) from the same batch (if requested)
    if exclude_source_batch_id is None:
        q = """SELECT reg_no, start_time, end_time, practical_code, source_batch_id
               FROM AssignmentsIndex WHERE date=?"""
        params = [date]
    else:
        q = """SELECT reg_no, start_time, end_time, practical_code, source_batch_id
               FROM AssignmentsIndex WHERE date=? AND source_batch_id<>?"""
        params = [date, exclude_source_batch_id]

    df = pd.read_sql_query(q, conn, params=params)
    conn.close()
    conflicts = {}
    for r in reg_nos:
        hits = []
        # ensure matching string types
        for _, row in df[df["reg_no"].astype(str) == str(r)].iterrows():
            if _overlap_or_gap_less_than(start_time, end_time, row["start_time"], row["end_time"], RULES["min_gap_minutes"]):
                hits.append((row["practical_code"], row["start_time"], row["end_time"]))
        if hits:
            conflicts[str(r)] = hits
    return conflicts

def get_student_existing_batches(reg_no):
    """Return list of dicts for a single student: [{'practical_code':..., 'date':..., 'start_time':..., 'end_time':...}, ...]"""
    init_db()
    conn = _connect()
    q = """SELECT practical_code, date, start_time, end_time FROM AssignmentsIndex WHERE reg_no=? ORDER BY date, time(start_time)"""
    df = pd.read_sql_query(q, conn, params=[str(reg_no)])
    conn.close()
    out = []
    for _, r in df.iterrows():
        out.append({
            "practical_code": r["practical_code"],
            "date": r["date"],
            "start_time": r["start_time"],
            "end_time": r["end_time"]
        })
    return out

def get_students_existing_batches(reg_nos):
    """Bulk helper: reg_nos -> list of existing assignments (for UI dropdown display)."""
    result = {}
    if not reg_nos:
        return result
    init_db()
    conn = _connect()
    placeholders = ",".join(["?"] * len(reg_nos))
    q = f"""SELECT reg_no, practical_code, date, start_time, end_time
            FROM AssignmentsIndex WHERE reg_no IN ({placeholders}) ORDER BY reg_no, date, time(start_time)"""
    df = pd.read_sql_query(q, conn, params=[str(r) for r in reg_nos])
    conn.close()
    for _, r in df.iterrows():
        reg = str(r["reg_no"])
        result.setdefault(reg, []).append({
            "practical_code": r["practical_code"],
            "date": r["date"],
            "start_time": r["start_time"],
            "end_time": r["end_time"]
        })
    return result

def _format_conflicts_for_message(conflicts_dict):
    """Human-readable text from conflicts dict."""
    lines = []
    for reg, hits in conflicts_dict.items():
        for pc, s, e in hits:
            lines.append(f"{reg} — {pc} ({s}-{e})")
    return "\n".join(lines)

def add_students_to_batch(batch_id, practical_code, reg_nos):
    """Adds students if size ≤ 30 and no duplicates.
    IMPORTANT: New behavior: if ANY selected student has a conflict, the operation will be blocked
    and NO selected students will be added. This follows the requested strict policy.
    Returns: (ok:bool, message:str, conflicts:dict)
      - ok = False when any conflict found (conflicts dict will be provided)
      - ok = True when all selected added successfully (conflicts will be empty)
    """
    init_db()
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT date,start_time,end_time FROM Batches WHERE batch_id=?", (batch_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return False, "Batch not found.", {}
    date, start_time, end_time = row

    # check capacity
    cur.execute("SELECT COUNT(*) FROM BatchMembers WHERE batch_id=?", (batch_id,))
    current = cur.fetchone()[0] or 0
    if current + len(reg_nos) > RULES["batch_size_max"]:
        conn.close()
        return False, f"Cannot exceed {RULES['batch_size_max']} students per batch.", {}

    # check conflicts BEFORE any insertion
    conflicts = check_conflicts_for_students(date, start_time, end_time, reg_nos)
    if conflicts:
        # Do NOT add any student if there are conflicts (strict blocking behavior)
        msg_text = "Conflict(s) detected. No students were added.\n\n" + _format_conflicts_for_message(conflicts)
        conn.close()
        return False, msg_text, conflicts

    # proceed to insert all (no conflicts)
    for r in reg_nos:
        try:
            cur.execute("""INSERT OR IGNORE INTO BatchMembers(batch_id,reg_no,practical_code)
                           VALUES(?,?,?)""", (batch_id, str(r), practical_code))
        except Exception:
            pass
    conn.commit()

    # update AssignmentsIndex for all added reg_nos (delete any stale entries for same source)
    for r in reg_nos:
        cur.execute("""DELETE FROM AssignmentsIndex WHERE reg_no=? AND source_batch_id=?""", (str(r), batch_id))
        cur.execute("""INSERT INTO AssignmentsIndex(reg_no,date,start_time,end_time,practical_code,source_batch_id)
                       VALUES(?,?,?,?,?,?)""", (str(r), date, start_time, end_time, practical_code, batch_id))
    conn.commit()
    conn.close()
    return True, f"Added {len(reg_nos)} student(s).", {}

def list_batch_members(batch_id, detailed=False):
    """If detailed=True, joins student name/dept."""
    conn = _connect()
    members = pd.read_sql_query("SELECT reg_no FROM BatchMembers WHERE batch_id=? ORDER BY reg_no", conn, params=[batch_id])
    conn.close()
    if not detailed:
        return members
    ssm = load_students()[["reg_no","student_name","dept_name"]].copy()
    ssm["reg_no"] = ssm["reg_no"].astype(str)
    members["reg_no"] = members["reg_no"].astype(str)
    return members.merge(ssm.drop_duplicates("reg_no"), on="reg_no", how="left")

def remove_student_from_batch(batch_id, reg_no):
    conn = _connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM BatchMembers WHERE batch_id=? AND reg_no=?", (batch_id, str(reg_no)))
    cur.execute("DELETE FROM AssignmentsIndex WHERE source_batch_id=? AND reg_no=?", (batch_id, str(reg_no)))
    conn.commit()
    conn.close()
    return True

# ---------- Backup ----------

def export_backup_excel():
    conn = _connect()
    batches = pd.read_sql_query("SELECT * FROM Batches ORDER BY practical_code, date, time(start_time)", conn)
    members = pd.read_sql_query("SELECT * FROM BatchMembers ORDER BY batch_id, reg_no", conn)
    conn.close()
    with pd.ExcelWriter(BACKUP_XLSX, engine="openpyxl") as writer:
        batches.to_excel(writer, index=False, sheet_name="Batches")
        members.to_excel(writer, index=False, sheet_name="BatchMembers")
    return BACKUP_XLSX
