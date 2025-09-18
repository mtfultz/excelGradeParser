#!/usr/bin/env python3

from __future__ import annotations
import os, sys, io, zipfile, secrets
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Dict, Any, Optional

import pandas as pd
from flask import (
    Flask, request, redirect, url_for, send_from_directory,
    render_template_string, abort, flash
)
from werkzeug.utils import secure_filename

# ----------------------------- Config ---------------------------------
APP_TITLE = "Excel Grade Parser"
RUNS_DIR = Path("runs")
ALLOWED_EXT = {".xlsx", ".xlsm"}  # convert legacy .xls to .xlsx first

REQUIRED_COLS  = ["CourseID","Course Name","Term","Section","Enroll","A","B","C","D","F","W","I"]
HEADER_MINIMUM = ["CourseID","Course Name","Term","Section","A","B","C","D","F"]
GRADE_COLS     = ["A","B","C","D","F","W","I"]
TARGET_KEYS    = ["CourseID","Course_Name","Term","Section"]

# ----------------------------- Flask ----------------------------------
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", secrets.token_hex(16))

# ----------------------------- Parser Core -----------------------------
def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = (out.columns
                   .str.strip()
                   .str.replace(r"\s+", "_", regex=True)
                   .str.replace(r"[^0-9A-Za-z_]+", "", regex=True))
    if "CourseName" in out.columns and "Course_Name" not in out.columns:
        out = out.rename(columns={"CourseName": "Course_Name"})
    return out

def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    dtype_map = {"CourseID":"string","Course_Name":"string","Term":"string","Section":"string","Enroll":"Int64"}
    for g in GRADE_COLS: dtype_map[g] = "Int64"
    for col, dt in dtype_map.items():
        if col in out.columns:
            try: out[col] = out[col].astype(dt)
            except Exception: pass
    return out

def detect_header_row(xlsx_path: Path, sheet_name: str, required_cols=HEADER_MINIMUM) -> int:
    tmp = pd.read_excel(xlsx_path, sheet_name=sheet_name, header=None, engine="openpyxl")
    req = {c.lower().replace(" ", "") for c in required_cols}
    for i, row in tmp.iterrows():
        labels = {str(v).strip().lower().replace(" ", "") for v in row.tolist() if pd.notna(v)}
        if req.issubset(labels): return i
    raise ValueError(
        f"Header row not found in {xlsx_path.name} / '{sheet_name}'. "
        f"Looked for labels like: {', '.join(required_cols)}."
    )

def load_sheet(xlsx_path: Path, sheet_name: str, header_row: Optional[int]) -> pd.DataFrame:
    if header_row is None:
        header_row = detect_header_row(xlsx_path, sheet_name)
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name, skiprows=header_row, engine="openpyxl")
    df = clean_columns(df)
    df = coerce_types(df)
    df["Sheet"] = sheet_name
    df["SourceFile"] = str(xlsx_path.name)
    return df

def resolve_sheet_filter(xl: pd.ExcelFile, sheets: Optional[List[str]]) -> List[str]:
    if not sheets: return xl.sheet_names
    names, resolved = xl.sheet_names, []
    for s in sheets:
        s = s.strip()
        if not s: continue
        if s.isdigit():
            idx = int(s)
            if idx < 0 or idx >= len(names): raise ValueError(f"Sheet index {idx} out of range 0..{len(names)-1}")
            resolved.append(names[idx])
        else:
            if s not in names: raise ValueError(f"Sheet '{s}' not in {xl.io}. Available: {names}")
            resolved.append(s)
    return resolved

def load_workbook(xlsx_path: Path, header_row: Optional[int], sheets: Optional[List[str]]) -> pd.DataFrame:
    xl = pd.ExcelFile(xlsx_path, engine="openpyxl")
    to_load = resolve_sheet_filter(xl, sheets)
    frames = [load_sheet(xlsx_path, s, header_row) for s in to_load]
    return pd.concat(frames, ignore_index=True)

def to_long(df: pd.DataFrame) -> pd.DataFrame:
    grade_cols = [c for c in GRADE_COLS if c in df.columns]
    id_vars = [c for c in df.columns if c not in grade_cols]
    out = df.melt(id_vars=id_vars, value_vars=grade_cols, var_name="Grade", value_name="Count")
    out["Count"] = out["Count"].fillna(0).astype("Int64")
    keys = [k for k in TARGET_KEYS if k in out.columns]
    totals = out.groupby(keys, dropna=False)["Count"].transform("sum")
    out["Pct"] = (out["Count"] / totals).astype(float)
    return out

def add_wide_checks(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    present = [c for c in GRADE_COLS if c in out.columns]
    if present:
        out["GradeTotal"] = out[present].sum(axis=1, numeric_only=True)
        if "Enroll" in out.columns:
            out["EnrollDiff"] = (out["Enroll"].astype("Float64") - out["GradeTotal"].astype("Float64"))
    return out

def validate_long(df_long: pd.DataFrame) -> pd.DataFrame:
    keys = [k for k in TARGET_KEYS if k in df_long.columns]
    agg = df_long.groupby(keys, dropna=False).agg(
        total_counts=("Count","sum"),
        pct_sum=("Pct","sum"),
        n_grade_bins=("Grade","nunique"),
    ).reset_index()
    agg["ok_pct_sum"] = agg["pct_sum"].round(3).between(0.99, 1.01)
    agg["ok_nonzero"] = agg["total_counts"] > 0
    return agg

def sheet_row_counts(df_wide: pd.DataFrame) -> pd.DataFrame:
    return df_wide.groupby(["SourceFile","Sheet"], dropna=False).size().reset_index(name="rows")

def try_write_parquet(df: pd.DataFrame, path: Path) -> bool:
    try:
        df.to_parquet(path, index=False); return True
    except Exception:
        return False

# ----------------------------- Orchestrator ----------------------------
def process_files(upload_paths: List[Path],
                  header_row: Optional[int],
                  sheets: Optional[List[str]],
                  run_dir: Path,
                  write_parquet: bool,
                  make_report: bool) -> Dict[str, Any]:
    run_dir.mkdir(parents=True, exist_ok=True)

    frames = [load_workbook(p, header_row, sheets) for p in upload_paths]
    df_wide = pd.concat(frames, ignore_index=True)
    df_wide = add_wide_checks(df_wide)
    df_long = to_long(df_wide)
    val = validate_long(df_long)

    out = {}
    out["wide_csv"]  = run_dir / "grades_combined.csv"
    out["long_csv"]  = run_dir / "grades_long.csv"
    # out["valid_csv"] = run_dir / "validation_by_section.csv"
    out["xlsx"]      = run_dir / "grades_combined_clean.xlsx"

    df_wide.to_csv(out["wide_csv"], index=False)
    df_long.to_csv(out["long_csv"], index=False)
    # val.to_csv(out["valid_csv"], index=False)
    with pd.ExcelWriter(out["xlsx"], engine="xlsxwriter") as writer:
        df_wide.to_excel(writer, sheet_name="AllData", index=False)



    # Build ZIP of outputs
    zip_path = run_dir / "outputs.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for p in out.values():
            z.write(p, arcname=p.name)
    out["zip"] = zip_path

    previews = {
        "counts": sheet_row_counts(df_wide).to_dict(orient="records"),
        "wide_html": df_wide.head(30).to_html(classes="table table-striped", index=False),
        "long_html": df_long.head(30).to_html(classes="table table-striped", index=False),
        # "valid_html": val.head(30).to_html(classes="table table-striped", index=False),
        "summary": {
            "wide_rows": int(len(df_wide)),
            "sections": int(df_long.groupby([k for k in TARGET_KEYS if k in df_long.columns]).ngroups),
            "failed_sections": int(val.query("~ok_pct_sum or ~ok_nonzero").shape[0]),
        }
    }
    return {"outputs": out, "previews": previews}

# ----------------------------- Templates -------------------------------
PAGE_BASE = """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{{ title }}</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Helvetica,Arial,sans-serif;margin:0;background:#0b0c10;color:#e5e7eb}
.container{max-width:1100px;margin:40px auto;padding:24px;background:#111827;border-radius:16px;box-shadow:0 10px 30px rgba(0,0,0,.3)}
h1{margin-top:0;font-size:24px}
a{color:#93c5fd}
.card{background:#0f172a;border:1px solid #1f2937;padding:16px;border-radius:12px;margin:12px 0}
label{display:block;margin:.25rem 0 .35rem}
input[type="number"],input[type="text"],select{width:100%;padding:.6rem;border-radius:8px;border:1px solid #374151;background:#111827;color:#e5e7eb}
input[type="file"]{width:100%;padding:.4rem;border-radius:8px;background:#0b0c10}
.btn{background:#2563eb;border:none;color:white;padding:.6rem 1rem;border-radius:10px;cursor:pointer}
.grid{display:grid;grid-template-columns: 1fr 1fr; gap:12px}
.table{width:100%;border-collapse:collapse}
.table th,.table td{border:1px solid #1f2937;padding:.35rem;text-align:left}
.badge{display:inline-block;padding:.2rem .5rem;border-radius:12px;background:#1f2937}
footer{opacity:.6;margin-top:1rem;font-size:12px}
.flash{background:#1f2937;border-left:4px solid #ef4444;padding:.5rem .75rem;border-radius:8px;margin:.5rem 0}
</style>
</head>
<body>
<div class="container">
{{ body|safe }}
</div>
</body>
</html>
"""

PAGE_INDEX = """
<h1>📄 {{ title }}</h1>
<div class="card">
  <form method="post" action="{{ url_for('process') }}" enctype="multipart/form-data">
    <label><strong>Upload Excel file(s)</strong> (.xlsx or .xlsm):</label>
    <input type="file" name="files" accept=".xlsx,.xlsm" multiple required>
    <div class="grid">
      <div>
        <label>Header row override (0-based, optional):</label>
        <input type="number" name="header_row" placeholder="e.g., 2">
      </div>
      <div>
        <label>Sheets to parse (names or indices, comma-separated; leave blank for all):</label>
        <input type="text" name="sheets" placeholder="Fall2024,Spring2025 or 0,1">
      </div>
    </div>
    <div class="grid">
      <div>
        <label>Preview type:</label>
        <select name="preview">
          <option value="wide" selected>Wide (recommended)</option>
          <option value="long">Long / tidy</option>
        </select>
      </div>
    </div>
    <div style="margin-top:10px;">
      <button class="btn" type="submit">Parse Files</button>
    </div>
  </form>
</div>
{% with messages = get_flashed_messages() %}
  {% if messages %}
    <div class="flash">{{ messages[0] }}</div>
  {% endif %}
{% endwith %}
"""

PAGE_RESULTS = """
<h1> Parsed {{ file_count }} file{{ '' if file_count==1 else 's' }}</h1>

<div class="card">
  <div><span class="badge">Rows:</span> {{ previews.summary.wide_rows | int }}</div>
  <div><span class="badge">Sections:</span> {{ previews.summary.sections | int }}</div>
  <div><span class="badge">Failed sections:</span> {{ previews.summary.failed_sections | int }}</div>
</div>

<div class="card">
  <h3>Parsed files & sheets</h3>
  <table class="table">
    <thead><tr><th>SourceFile</th><th>Sheet</th><th>Rows</th></tr></thead>
    <tbody>
      {% for r in previews.counts %}
      <tr><td>{{ r.SourceFile }}</td><td>{{ r.Sheet }}</td><td>{{ r.rows }}</td></tr>
      {% endfor %}
    </tbody>
  </table>
</div>

<div class="card">
  {% if preview == 'long' %}
    <h3>Long / tidy preview</h3>
    {{ previews.long_html | safe }}
  {% elif preview == 'validation' %}
    <h3>Validation preview</h3>
    {{ previews.valid_html | safe }}
  {% else %}
    <h3>Wide preview</h3>
    {{ previews.wide_html | safe }}
  {% endif %}
</div>

<div class="card">
  <h3>Downloads</h3>
  <ul>
    {% for name, path in outputs.items() %}
      <li><a href="{{ url_for('download', run_id=run_id, filename=path.name) }}">{{ path.name }}</a></li>
    {% endfor %}
  </ul>
  <p><a href="{{ url_for('index') }}">← Parse more files</a></p>
</div>
"""

# ----------------------------- Routes ---------------------------------
@app.route("/", methods=["GET"])
def index():
    body = render_template_string(PAGE_INDEX, title=APP_TITLE)
    return render_template_string(PAGE_BASE, title=APP_TITLE, body=body)

def _parse_sheets_field(raw: str) -> Optional[List[str]]:
    if not raw: return None
    parts = [p.strip() for p in raw.split(",")]
    parts = [p for p in parts if p]
    return parts or None

@app.route("/process", methods=["POST"])
def process():
    files = request.files.getlist("files")
    if not files:
        flash("Please select at least one Excel file.")
        return redirect(url_for("index"))

    header_row = request.form.get("header_row", "").strip()
    header_row_val = int(header_row) if header_row.isdigit() else None
    sheets = _parse_sheets_field(request.form.get("sheets", ""))
    preview_choice = request.form.get("preview", "wide")
    write_report = bool(request.form.get("report"))
    write_parquet = bool(request.form.get("parquet"))

    run_id = datetime.utcnow().strftime("%Y%m%d-%H%M%S") + "-" + secrets.token_hex(4)
    run_dir = RUNS_DIR / run_id
    uploads_dir = run_dir / "uploads"
    uploads_dir.mkdir(parents=True, exist_ok=True)

    upload_paths = []
    for f in files:
        if not f or f.filename == "": continue
        name = secure_filename(f.filename)
        ext = Path(name).suffix.lower()
        if ext not in ALLOWED_EXT:
            flash(f"Unsupported file type: {name}"); continue
        p = uploads_dir / name
        f.save(p)
        upload_paths.append(p)

    if not upload_paths:
        flash("No valid .xlsx/.xlsm files uploaded.")
        return redirect(url_for("index"))

    try:
        result = process_files(upload_paths, header_row_val, sheets, run_dir, write_parquet, write_report)
    except Exception as e:
        print("Processing error:", e, file=sys.stderr)
        flash(f"Error parsing files: {e}")
        return redirect(url_for("index"))

    body = render_template_string(PAGE_RESULTS,
                                  run_id=run_id,
                                  outputs=result["outputs"],
                                  previews=result["previews"],
                                  file_count=len(upload_paths),
                                  preview=preview_choice)
    return render_template_string(PAGE_BASE, title=APP_TITLE, body=body)

@app.route("/download/<run_id>/<path:filename>", methods=["GET"])
def download(run_id, filename):
    base = RUNS_DIR / run_id
    if not base.exists(): abort(404)
    target = base / filename
    if not target.exists() or not target.is_file(): abort(404)
    return send_from_directory(base, filename, as_attachment=True)

@app.route("/download_zip/<run_id>", methods=["GET"])
def download_zip(run_id):
    base = RUNS_DIR / run_id
    if not base.exists(): abort(404)
    z = base / "outputs.zip"
    if not z.exists(): abort(404)
    return send_from_directory(base, "outputs.zip", as_attachment=True)

# ----------------------------- Main -----------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
