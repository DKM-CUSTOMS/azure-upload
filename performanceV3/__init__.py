"""
performanceV3 - Azure Function
==============================
Data architecture that avoids memory crashes on the Azure Consumption plan by
pre-computing everything into small cache files. The GET endpoints serve those
caches instantly with no calculation at request time.

DAILY cold rebuild (run on a schedule, e.g. 2 AM):
    POST /transform-daily        raw landing JSON   -> one daily parquet
    POST /build-index            all daily parquets -> file_index.parquet
    POST /refresh-users          all daily parquets -> per-user caches (single pass)
    POST /refresh                last 10 working days -> 10-day summary cache

  Backfill helper:
    POST /transform-daily-range?start=YYYY-MM-DD&end=YYYY-MM-DD  (idempotent, resumable)

INSTANT GET endpoints (always read from cache — no calculation):
    GET /                        -> 10-day summary cache
    GET ?user=X                  -> per-user cache
    GET users                    -> discovered users (from the index)
    GET file-lifecycle?id=...    -> single declaration trace

Performance model: every heavy job reads each data file exactly once (parallel,
column-pruned) and computes all users in a single pass. All jobs only READ
landing/transformed data and WRITE regenerable caches/index — source landing
JSON and daily parquets are never modified.
"""

from collections import defaultdict
from datetime import datetime, timedelta
import azure.functions as func
import logging
import json
import io
import pandas as pd
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

from performanceV3.common import classify_file_activity, SENDING_STATUSES, SYSTEM_USERS

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KEY_VAULT_URL = "https://kv-functions-python.vault.azure.net"
SECRET_NAME = "azure-storage-account-access-key2"
CONTAINER_NAME = "document-intelligence"
BLOB_BASE = "streamliner-analytics"  # base prefix inside the container

# Raw landing files (written by Logic App)
LANDING_PREFIX = f"{BLOB_BASE}/landing/euchistory/"

# Daily transformed parquet files
TRANSFORMED_PREFIX = f"{BLOB_BASE}/transformed/"

# Pre-computed file index (one row per DECLARATIONID)
INDEX_BLOB_PATH = f"{BLOB_BASE}/index/file_index.parquet"

# Cache paths (instant-read endpoints)
SUMMARY_BLOB_PATH = f"Dashboard/cache/users_summaryV3.json"
USER_CACHE_PATH_PREFIX = "Dashboard/cache/usersV3/"

# Import declaration types — used to determine team membership
IMPORT_TYPES = {"DMS_IMPORT", "IDMS_IMPORT"}

# Columns required by every analytics computation. Used for column-pruned parquet
# reads so we download/hold only what we need (big memory + I/O win on full scans).
NEEDED_COLS = [
    "DECLARATIONID", "USERCODE", "HISTORY_STATUS", "HISTORYDATETIME",
    "ACTIVECOMPANY", "TYPEDECLARATIONSSW", "PRINCIPAL",
]

# ---------------------------------------------------------------------------
# Azure Services Initialization
# ---------------------------------------------------------------------------
try:
    credential = DefaultAzureCredential()
    kv_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)
    connection_string = kv_client.get_secret(SECRET_NAME).value
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
except Exception as e:
    logging.critical(f"Failed to initialize Azure services: {e}")
    connection_string = None
    blob_service_client = None


# ---------------------------------------------------------------------------
# Helper: generic blob read / write
# ---------------------------------------------------------------------------

def _blob_client(path: str):
    return blob_service_client.get_blob_client(CONTAINER_NAME, path)


def _read_parquet(path: str, columns=None) -> pd.DataFrame:
    """Read a single parquet file from blob storage into a DataFrame.

    When ``columns`` is provided, only the requested columns that actually exist
    in the file are read (column-pruned read) — this drastically cuts download
    size and memory. Columns missing from the file are ignored, so callers may
    safely pass a superset.
    """
    bc = _blob_client(path)
    if not bc.exists():
        logging.warning(f"Parquet not found: {path}")
        return pd.DataFrame()
    data = bc.download_blob().readall()
    buf = io.BytesIO(data)
    if columns:
        available = set(pq.read_schema(buf).names)
        cols = [c for c in columns if c in available]
        buf.seek(0)
        return pd.read_parquet(buf, columns=cols)
    return pd.read_parquet(buf)


def _write_parquet(df: pd.DataFrame, path: str):
    """Write a DataFrame as parquet to blob storage."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    _blob_client(path).upload_blob(buf.getvalue(), overwrite=True)
    logging.info(f"Saved parquet → {path}")


def _read_json_blob(path: str):
    """Read a JSON blob and return as Python object. Returns None if missing."""
    bc = _blob_client(path)
    if not bc.exists():
        return None
    return json.loads(bc.download_blob().readall())


def _write_json_blob(data, path: str):
    """Write a Python object as JSON to blob storage."""
    _blob_client(path).upload_blob(json.dumps(data, indent=2), overwrite=True)
    logging.info(f"Saved JSON → {path}")


def _list_blobs(prefix: str):
    """Return list of blob names under a given prefix."""
    container: ContainerClient = blob_service_client.get_container_client(CONTAINER_NAME)
    return [b.name for b in container.list_blobs(name_starts_with=prefix)]


def _download_json_frames(blob_names: list) -> list:
    """Download and parse many landing JSON blobs in parallel.

    Returns a list of DataFrames (one per non-empty blob) in the SAME order as
    ``blob_names`` so downstream concat/dedup behaves exactly like the previous
    sequential version — only faster (downloads run concurrently).
    """
    if not blob_names:
        return []

    def _load(blob_name):
        try:
            raw = _blob_client(blob_name).download_blob().readall()
            records = json.loads(raw)
            if isinstance(records, list) and records:
                return pd.DataFrame(records)
        except Exception as e:
            logging.error(f"Failed to read blob {blob_name}: {e}")
        return None

    frames = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for df in ex.map(_load, blob_names):
            if df is not None:
                frames.append(df)
    return frames


def _read_all_transformed_df(columns=None) -> pd.DataFrame:
    """Read every transformed daily parquet once, in parallel, into one frame.

    Reads are column-pruned (via ``columns``) and run concurrently, but results
    are concatenated in blob-name order so any tie-break-sensitive downstream
    logic (e.g. stable sorts on equal timestamps) stays deterministic.
    """
    parquet_blobs = [b for b in _list_blobs(TRANSFORMED_PREFIX) if b.endswith(".parquet")]
    if not parquet_blobs:
        return pd.DataFrame()

    def _load(blob_name):
        try:
            return _read_parquet(blob_name, columns=columns)
        except Exception as e:
            logging.error(f"Failed to read parquet {blob_name}: {e}")
            return None

    frames = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for df in ex.map(_load, parquet_blobs):
            if df is not None and not df.empty:
                frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Helper: get last N working days
# ---------------------------------------------------------------------------

def _last_n_working_days(n: int):
    days = []
    curr = datetime.utcnow().date()
    while len(days) < n:
        if curr.weekday() < 5:  # Mon–Fri
            days.insert(0, curr)
        curr -= timedelta(days=1)
    return days


# ---------------------------------------------------------------------------
# Helper: daily parquet path
# ---------------------------------------------------------------------------

def _daily_parquet_path(day) -> str:
    """day can be a date object or a datetime."""
    return f"{TRANSFORMED_PREFIX}year={day.year}/month={day.month:02d}/day={day.day:02d}/data.parquet"


# ===========================================================================
# ROUTE 1 – POST /transform-daily
# Reads raw landing JSON files for a specific day (or today) and writes a
# compressed daily parquet file.
# ===========================================================================

def transform_daily(req: func.HttpRequest) -> func.HttpResponse:
    """
    Query params:
      ?date=YYYY-MM-DD   (optional, defaults to yesterday UTC)
      ?force=true        (optional, overwrite existing parquet)
    """
    date_str = req.params.get("date")
    force = req.params.get("force", "false").lower() == "true"

    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else (datetime.utcnow().date() - timedelta(days=1))
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Invalid date format. Use YYYY-MM-DD"}),
            status_code=400, mimetype="application/json"
        )

    out_path = _daily_parquet_path(target_date)

    # Skip if already transformed and force is not set
    if not force and _blob_client(out_path).exists():
        return func.HttpResponse(
            json.dumps({"status": "skipped", "message": f"Parquet already exists for {target_date}. Use ?force=true to overwrite."}),
            status_code=200, mimetype="application/json"
        )

    day_prefix = f"{LANDING_PREFIX}day={target_date.isoformat()}/"
    json_blobs = _list_blobs(day_prefix)

    if not json_blobs:
        return func.HttpResponse(
            json.dumps({"status": "no_data", "message": f"No landing JSON files found for {target_date} under {day_prefix}"}),
            status_code=200, mimetype="application/json"
        )

    logging.info(f"transform-daily: found {len(json_blobs)} JSON files for {target_date}")

    frames = _download_json_frames(json_blobs)

    if not frames:
        return func.HttpResponse(
            json.dumps({"status": "no_data", "message": f"All JSON files for {target_date} were empty or unreadable."}),
            status_code=200, mimetype="application/json"
        )

    df = pd.concat(frames, ignore_index=True)

    # Standardise columns
    for col in ["USERCODE", "HISTORY_STATUS", "ACTIVECOMPANY", "TYPEDECLARATIONSSW", PRINCIPAL_FIELD]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    df["HISTORYDATETIME"] = pd.to_datetime(df["HISTORYDATETIME"], errors="coerce", format="mixed")

    # Remove duplicates
    dup_cols = [c for c in ["DECLARATIONID", "USERCODE", "HISTORY_STATUS", "HISTORYDATETIME"] if c in df.columns]
    df = df.drop_duplicates(subset=dup_cols)

    _write_parquet(df, out_path)

    return func.HttpResponse(
        json.dumps({
            "status": "success",
            "date": str(target_date),
            "rows_written": len(df),
            "files_processed": len(frames),
            "output_path": out_path
        }),
        status_code=200, mimetype="application/json"
    )


# ===========================================================================
# ROUTE 1b – POST /transform-daily-range
# Batch version of transform-daily. Processes every day in a date range.
# Perfect for backfilling historical data in one single API call.
#
# Params:
#   ?start=YYYY-MM-DD   (required) first day to process
#   ?end=YYYY-MM-DD     (optional, defaults to today)
#   ?force=true         (optional, overwrite days that already have a parquet)
# ===========================================================================

def _transform_one_day(target_date, force: bool) -> dict:
    """
    Core logic for a single day's transform. Returns a result dict.
    Extracted so it can be called from both transform_daily and transform_daily_range.
    """
    out_path = _daily_parquet_path(target_date)
    date_str = str(target_date)

    if not force and _blob_client(out_path).exists():
        return {"date": date_str, "status": "skipped", "reason": "parquet already exists"}

    day_prefix = f"{LANDING_PREFIX}day={date_str}/"
    json_blobs = _list_blobs(day_prefix)

    if not json_blobs:
        return {"date": date_str, "status": "no_data", "reason": "no landing JSON files found"}

    frames = _download_json_frames(json_blobs)

    if not frames:
        return {"date": date_str, "status": "no_data", "reason": "all JSON files were empty or unreadable"}

    df = pd.concat(frames, ignore_index=True)
    for col in ["USERCODE", "HISTORY_STATUS", "ACTIVECOMPANY", "TYPEDECLARATIONSSW", PRINCIPAL_FIELD]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()
    df["HISTORYDATETIME"] = pd.to_datetime(df["HISTORYDATETIME"], errors="coerce", format="mixed")
    dup_cols = [c for c in ["DECLARATIONID", "USERCODE", "HISTORY_STATUS", "HISTORYDATETIME"] if c in df.columns]
    df = df.drop_duplicates(subset=dup_cols)
    _write_parquet(df, out_path)

    return {
        "date": date_str,
        "status": "success",
        "rows_written": len(df),
        "files_read": len(frames)
    }


def transform_daily_range(req: func.HttpRequest) -> func.HttpResponse:
    """
    Process all days from ?start= to ?end= (inclusive).
    Use this to backfill historical data in one call.
    """
    start_str = req.params.get("start")
    end_str = req.params.get("end")
    force = req.params.get("force", "false").lower() == "true"

    if not start_str:
        return func.HttpResponse(
            json.dumps({"error": "Missing required param: ?start=YYYY-MM-DD"}),
            status_code=400, mimetype="application/json"
        )

    try:
        start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_str, "%Y-%m-%d").date() if end_str else datetime.utcnow().date()
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Invalid date format. Use YYYY-MM-DD"}),
            status_code=400, mimetype="application/json"
        )

    if start_date > end_date:
        return func.HttpResponse(
            json.dumps({"error": f"start ({start_date}) must be before or equal to end ({end_date})"}),
            status_code=400, mimetype="application/json"
        )

    # Build list of all days in the range
    all_days = []
    curr = start_date
    while curr <= end_date:
        all_days.append(curr)
        curr += timedelta(days=1)

    logging.info(f"transform-daily-range: processing {len(all_days)} days ({start_date} → {end_date}), force={force}")

    results = []
    success_count = 0
    skipped_count = 0
    no_data_count = 0

    for day in all_days:
        try:
            result = _transform_one_day(day, force)
            results.append(result)
            if result["status"] == "success":
                success_count += 1
                logging.info(f"  ✔ {day}: {result.get('rows_written', 0)} rows")
            elif result["status"] == "skipped":
                skipped_count += 1
                logging.info(f"  ⏩ {day}: skipped (already exists)")
            else:
                no_data_count += 1
                logging.info(f"  ∅ {day}: no data")
        except Exception as e:
            logging.error(f"  ✘ {day}: error — {e}")
            results.append({"date": str(day), "status": "error", "reason": str(e)})

    return func.HttpResponse(
        json.dumps({
            "status": "done",
            "range": f"{start_date} → {end_date}",
            "total_days": len(all_days),
            "success": success_count,
            "skipped": skipped_count,
            "no_data": no_data_count,
            "per_day": results
        }),
        status_code=200, mimetype="application/json"
    )


# Reads ALL daily parquet files and builds/updates the file_index.parquet.
# The index has ONE row per DECLARATIONID with pre-computed flags.
# ===========================================================================
PRINCIPAL_FIELD = "PRINCIPAL"    # principal field from euchistory data

def build_index(req: func.HttpRequest) -> func.HttpResponse:
    """
    Reads all transformed/year=.../month=.../day=.../data.parquet files,
    groups by DECLARATIONID, and writes index/file_index.parquet.
    """
    logging.info("build-index: starting full scan of transformed parquet files")

    parquet_blobs = [b for b in _list_blobs(TRANSFORMED_PREFIX) if b.endswith(".parquet")]

    if not parquet_blobs:
        return func.HttpResponse(
            json.dumps({"status": "no_data", "message": "No transformed parquet files found."}),
            status_code=200, mimetype="application/json"
        )

    logging.info(f"build-index: loading {len(parquet_blobs)} parquet files (parallel, column-pruned)")

    df = _read_all_transformed_df(columns=NEEDED_COLS)

    if df.empty:
        return func.HttpResponse(
            json.dumps({"error": "Could not read any transformed parquet files."}),
            status_code=500, mimetype="application/json"
        )

    # Standardise
    for col in ["USERCODE", "HISTORY_STATUS", "ACTIVECOMPANY", "TYPEDECLARATIONSSW", PRINCIPAL_FIELD]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    df["HISTORYDATETIME"] = pd.to_datetime(df["HISTORYDATETIME"], errors="coerce", format="mixed")
    df = df.dropna(subset=["HISTORYDATETIME", "DECLARATIONID"])

    # Filter out DKM_VP
    if "ACTIVECOMPANY" in df.columns:
        df = df[df["ACTIVECOMPANY"] != "DKM_VP"]

    logging.info(f"build-index: total rows loaded = {len(df)}, building index…")

    index_rows = []
    system_users = {"BATCHPROC", "ADMIN", "SYSTEM", "BATCH_PROC"}

    for decl_id, group in df.groupby("DECLARATIONID"):
        group = group.sort_values("HISTORYDATETIME")

        statuses = group["HISTORY_STATUS"].tolist()
        users = group["USERCODE"].tolist()

        has_interface = "INTERFACE" in set(statuses)
        has_manual_trigger = bool({"COPIED", "COPY", "NEW"}.intersection(set(statuses)))

        first_seen = group["HISTORYDATETIME"].min().date().isoformat()
        last_seen = group["HISTORYDATETIME"].max().date().isoformat()

        company = str(group["ACTIVECOMPANY"].iloc[0]) if "ACTIVECOMPANY" in group.columns else ""
        principal = str(group[PRINCIPAL_FIELD].iloc[0]) if PRINCIPAL_FIELD in group.columns else ""
        type_val = str(group["TYPEDECLARATIONSSW"].iloc[0]) if "TYPEDECLARATIONSSW" in group.columns else ""

        unique_users = list(group["USERCODE"].unique())
        human_users = [u for u in unique_users if u not in system_users]

        # created_by = first user action
        created_by = str(group.iloc[0]["USERCODE"])

        # modified_by = human with most MODIFIED actions (fallback to first human, then created_by)
        human_actions = group[~group["USERCODE"].isin(system_users)]
        mod_rows = human_actions[human_actions["HISTORY_STATUS"] == "MODIFIED"]
        if not mod_rows.empty:
            modified_by = str(mod_rows["USERCODE"].value_counts().idxmax())
        elif not human_actions.empty:
            modified_by = str(human_actions.iloc[0]["USERCODE"])
        else:
            modified_by = created_by

        # file_creation_duration: hours from first MODIFIED → first WRT_ENT
        session_start = None
        file_creation_duration = None
        for _, row in group.iterrows():
            if row["HISTORY_STATUS"] == "MODIFIED" and session_start is None:
                session_start = row["HISTORYDATETIME"]
            elif row["HISTORY_STATUS"] == "WRT_ENT" and session_start is not None:
                file_creation_duration = round((row["HISTORYDATETIME"] - session_start).total_seconds() / 3600, 3)
                break

        sending_count = int((group["HISTORY_STATUS"] == "DEC_DAT").sum())
        modification_count = int((group["HISTORY_STATUS"] == "MODIFIED").sum())

        index_rows.append({
            "DECLARATIONID": decl_id,
            "first_seen": first_seen,
            "last_seen": last_seen,
            "principal": principal,
            "company": company,
            "users": json.dumps(unique_users),       # stored as JSON string in parquet
            "human_users": json.dumps(human_users),
            "has_interface": has_interface,
            "has_manual_trigger": has_manual_trigger,
            "created_by": created_by,
            "modified_by": modified_by,
            "file_creation_duration": file_creation_duration,
            "statuses": json.dumps(list(set(statuses))),
            "sending_count": sending_count,
            "modification_count": modification_count,
            "type": type_val,
        })

    index_df = pd.DataFrame(index_rows)
    _write_parquet(index_df, INDEX_BLOB_PATH)

    return func.HttpResponse(
        json.dumps({
            "status": "success",
            "total_declarations_indexed": len(index_df),
            "source_files": len(parquet_blobs),
            "index_path": INDEX_BLOB_PATH
        }),
        status_code=200, mimetype="application/json"
    )


# ===========================================================================
# ROUTE 3 – POST /refresh-users
# ===========================================================================
# Single-pass strategy: read ALL transformed daily parquets ONCE (column-pruned,
# in parallel), then slice that in-memory frame per user and feed each slice to
# the UNCHANGED _compute_rich_user_metrics. This produces byte-identical output
# to the previous per-user version but downloads each parquet once instead of
# once per user (the old path re-read the same ~40 files for every user).
# ===========================================================================

def refresh_users(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("refresh-users: starting rich user metrics rebuild (single-pass)")

    full_df = _read_all_transformed_df(columns=NEEDED_COLS)
    if full_df.empty:
        return func.HttpResponse(
            json.dumps({"status": "skipped", "message": "No transformed parquet data found. Run /transform-daily first."}),
            status_code=200, mimetype="application/json"
        )

    # Reduce to exactly the same universe the index (and therefore the previous
    # path) used: standardise text columns, require a valid datetime/declaration,
    # and drop DKM_VP. This guarantees the discovered user set and per-user
    # declaration sets match the old index-driven behaviour.
    for col in ["USERCODE", "HISTORY_STATUS", "ACTIVECOMPANY", "TYPEDECLARATIONSSW", PRINCIPAL_FIELD]:
        if col in full_df.columns:
            full_df[col] = full_df[col].astype(str).str.strip().str.upper()
    full_df["HISTORYDATETIME"] = pd.to_datetime(full_df["HISTORYDATETIME"], errors="coerce", format="mixed")
    full_df = full_df.dropna(subset=["HISTORYDATETIME", "DECLARATIONID"])
    if "ACTIVECOMPANY" in full_df.columns:
        full_df = full_df[full_df["ACTIVECOMPANY"] != "DKM_VP"]
    full_df["DECLARATIONID"] = full_df["DECLARATIONID"].astype(str)

    if full_df.empty:
        return func.HttpResponse(
            json.dumps({"status": "skipped", "message": "No usable rows after cleaning."}),
            status_code=200, mimetype="application/json"
        )

    # user -> array(DECLARATIONID) in one vectorized pass (replaces the per-user
    # index .apply scan that was O(users x declarations)).
    user_to_decls = full_df.groupby("USERCODE")["DECLARATIONID"].unique()
    all_users = sorted(
        u for u in user_to_decls.index
        if u not in SYSTEM_USERS and u not in ("NAN", "NONE", "")
    )
    logging.info(f"refresh-users: discovered {len(all_users)} human users from {len(full_df)} rows")

    processed_count = 0
    failed_count = 0

    for user in all_users:
        try:
            decl_ids = set(user_to_decls.loc[user])
            # Slice the in-memory frame (no blob re-reads). _compute_rich_user_metrics
            # copies and re-cleans internally, so this is the exact same input the
            # old path built from per-day parquet reads.
            user_df = full_df[full_df["DECLARATIONID"].isin(decl_ids)]

            metrics = _compute_rich_user_metrics(user_df, user)

            _write_json_blob(metrics, f"{USER_CACHE_PATH_PREFIX}{user}.json")
            processed_count += 1
            logging.info(f"refresh-users: ✔ cached {user} — "
                         f"{metrics['summary'].get('total_files_handled', 0)} files handled")

        except Exception as e:
            failed_count += 1
            logging.error(f"refresh-users: ✘ failed for {user}: {e}", exc_info=True)

    return func.HttpResponse(
        json.dumps({
            "status": "success",
            "processed": processed_count,
            "failed": failed_count,
            "total_users_discovered": len(all_users),
            "users": all_users
        }),
        status_code=200, mimetype="application/json"
    )


def _safe_json_loads(val):
    try:
        return json.loads(val) if isinstance(val, str) else val
    except Exception:
        return []


def _compute_rich_user_metrics(df: pd.DataFrame, username: str) -> dict:
    """
    Compute full rich user metrics from raw history rows — same format as V2.

    df = all rows for declarations this user is involved in (pre-filtered).
    Produces:
      - daily_metrics: per-day breakdown with file IDs
      - summary: totals, activity_by_hour, company_specialization, inactivity_days, etc.
    """
    from collections import defaultdict

    username_upper = username.upper()

    # -----------------------------------------------------------------------
    # Normalise the DataFrame
    # -----------------------------------------------------------------------
    df = df.copy()
    for col in ["USERCODE", "HISTORY_STATUS", "ACTIVECOMPANY", "TYPEDECLARATIONSSW", PRINCIPAL_FIELD]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    df["HISTORYDATETIME"] = pd.to_datetime(df["HISTORYDATETIME"], errors="coerce", format="mixed")
    df = df.dropna(subset=["HISTORYDATETIME"])
    df["HISTORYDATETIME"] = df["HISTORYDATETIME"].dt.tz_localize(None)
    df["DECLARATIONID"] = df["DECLARATIONID"].astype(str)

    # -----------------------------------------------------------------------
    # Global user-level stats (from all rows where user is the actor)
    # -----------------------------------------------------------------------
    user_actions = df[df["USERCODE"] == username_upper]

    # activity_by_hour
    activity_by_hour = {}
    if not user_actions.empty:
        hours = user_actions["HISTORYDATETIME"].dt.hour.value_counts().to_dict()
        activity_by_hour = {str(k): int(v) for k, v in sorted(hours.items())}

    # activity_days (date → row count for that user)
    activity_days = {}
    if not user_actions.empty:
        day_counts = user_actions.groupby(
            user_actions["HISTORYDATETIME"].dt.date
        ).size().to_dict()
        activity_days = {d.isoformat(): int(c) for d, c in day_counts.items()}

    # company_specialization (which company the user works for most)
    company_spec = {}
    if "ACTIVECOMPANY" in df.columns and not user_actions.empty:
        comp_counts = user_actions["ACTIVECOMPANY"].value_counts().to_dict()
        company_spec = {
            k: int(v) for k, v in comp_counts.items()
            if k not in ("NAN", "NONE", "", "DKM_VP")
        }

    # principal_specialization (which client/principal the user handles most)
    principal_spec = {}
    if PRINCIPAL_FIELD in df.columns and not user_actions.empty:
        principal_counts = user_actions[PRINCIPAL_FIELD].value_counts().to_dict()
        principal_spec = {
            k: int(v) for k, v in principal_counts.items()
            if k not in ("NAN", "NONE", "", "0", "0.0")
        }

    # file_type_counts
    file_type_counts = {}
    if "TYPEDECLARATIONSSW" in df.columns and not user_actions.empty:
        type_counts = user_actions["TYPEDECLARATIONSSW"].value_counts().to_dict()
        file_type_counts = {
            k: int(v) for k, v in type_counts.items()
            if k not in ("NAN", "NONE", "")
        }

    # -----------------------------------------------------------------------
    # Per-declaration analysis
    # -----------------------------------------------------------------------
    # daily_data[date_str] accumulates what happened per day
    daily_data = defaultdict(lambda: {
        "manual_files_created": 0,
        "automatic_files_created": 0,
        "modification_count": 0,
        "modification_file_ids": [],
        "manual_file_ids": [],
        "automatic_file_ids": [],
        "creation_durations": [],
        "deleted_file_ids": [],
        "deleted_own_file_ids": [],
        "deleted_others_file_ids": [],
    })

    total_manual = 0
    total_automatic = 0
    total_modifications = 0
    total_deletions = 0
    total_deleted_own = 0
    total_deleted_others = 0
    total_deleted_manual = 0
    total_deleted_automatic = 0
    all_durations = []

    for decl_id, group in df.groupby("DECLARATIONID"):
        group = group.sort_values("HISTORYDATETIME")
        user_decl_rows = group[group["USERCODE"] == username_upper]
        if user_decl_rows.empty:
            continue

        global_statuses = group["HISTORY_STATUS"].tolist()
        user_statuses = user_decl_rows["HISTORY_STATUS"].tolist()

        # Classify manual / automatic using the shared logic
        is_manual, is_automatic = classify_file_activity(
            global_history=global_statuses,
            user_history=user_statuses,
            group_df=group,
            target_user=username_upper,
            prefer_creation_status_owner=True
        )

        # Date of user's FIRST action on this declaration
        first_user_action_date = user_decl_rows["HISTORYDATETIME"].min().date().isoformat()

        if is_manual:
            total_manual += 1
            daily_data[first_user_action_date]["manual_files_created"] += 1
            daily_data[first_user_action_date]["manual_file_ids"].append(decl_id)
        elif is_automatic:
            total_automatic += 1
            daily_data[first_user_action_date]["automatic_files_created"] += 1
            daily_data[first_user_action_date]["automatic_file_ids"].append(decl_id)

        # Modification tracking per day
        user_mods = user_decl_rows[user_decl_rows["HISTORY_STATUS"] == "MODIFIED"]
        if not user_mods.empty:
            for mod_date, mod_group in user_mods.groupby(
                user_mods["HISTORYDATETIME"].dt.date
            ):
                day_str = mod_date.isoformat()
                daily_data[day_str]["modification_count"] += len(mod_group)
                if decl_id not in daily_data[day_str]["modification_file_ids"]:
                    daily_data[day_str]["modification_file_ids"].append(decl_id)
            total_modifications += len(user_mods)

        # Deletion tracking: a file counts as deleted only when the FINAL
        # status in the declaration history is DELETED. Credit goes only to
        # the user who performed that final deletion event.
        final_row = group.iloc[-1]
        final_status = str(final_row["HISTORY_STATUS"]).upper()
        final_user = str(final_row["USERCODE"]).upper()

        if final_status == "DELETED" and final_user == username_upper:
            deletion_date = final_row["HISTORYDATETIME"].date().isoformat()

            total_deletions += 1
            daily_data[deletion_date]["deleted_file_ids"].append(decl_id)

            if is_manual or is_automatic:
                # User deleted their own file (they get credit for creating it)
                total_deleted_own += 1
                daily_data[deletion_date]["deleted_own_file_ids"].append(decl_id)
            else:
                # User deleted someone else's file
                total_deleted_others += 1
                daily_data[deletion_date]["deleted_others_file_ids"].append(decl_id)

            if is_manual:
                total_deleted_manual += 1
            elif is_automatic:
                total_deleted_automatic += 1

        # File creation duration: user's first MODIFIED → global WRT_ENT
        session_start = None
        duration = None
        for _, row in group.iterrows():
            if row["HISTORY_STATUS"] == "MODIFIED" and session_start is None:
                if row["USERCODE"] == username_upper:
                    session_start = row["HISTORYDATETIME"]
            elif row["HISTORY_STATUS"] == "WRT_ENT" and session_start is not None:
                duration = round(
                    (row["HISTORYDATETIME"] - session_start).total_seconds() / 3600, 3
                )
                break

        if duration is not None:
            all_durations.append(duration)
            if is_manual or is_automatic:
                daily_data[first_user_action_date]["creation_durations"].append(duration)

    # -----------------------------------------------------------------------
    # Build daily_metrics list
    # -----------------------------------------------------------------------
    total_files_handled = total_manual + total_automatic

    daily_metrics = []
    for date_str, data in sorted(daily_data.items()):
        durations = data["creation_durations"]
        avg_time = round(sum(durations) / len(durations), 3) if durations else None
        day_total = data["manual_files_created"] + data["automatic_files_created"]
        daily_metrics.append({
            "date": date_str,
            "manual_files_created": data["manual_files_created"],
            "automatic_files_created": data["automatic_files_created"],
            "modification_count": data["modification_count"],
            "modification_file_ids": data["modification_file_ids"],
            "total_files_handled": day_total,
            "avg_creation_time": avg_time,
            "manual_file_ids": data["manual_file_ids"],
            "automatic_file_ids": data["automatic_file_ids"],
            "deleted_file_ids": data["deleted_file_ids"],
            "deleted_own_file_ids": data["deleted_own_file_ids"],
            "deleted_others_file_ids": data["deleted_others_file_ids"],
        })

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    avg_creation_time = (
        round(sum(all_durations) / len(all_durations), 3) if all_durations else None
    )

    days_with_files = [d for d in daily_metrics if d["total_files_handled"] > 0]
    days_active = len(set(activity_days.keys()))
    avg_files_per_day = (
        round(total_files_handled / len(days_with_files), 2) if days_with_files else 0
    )

    most_productive_day = None
    if days_with_files:
        most_productive_day = max(
            days_with_files, key=lambda x: x["total_files_handled"]
        )["date"]

    # Inactivity days (working days between first and last active date with no activity)
    inactivity_days = []
    if activity_days:
        from_date = datetime.strptime(min(activity_days.keys()), "%Y-%m-%d").date()
        to_date = datetime.strptime(max(activity_days.keys()), "%Y-%m-%d").date()
        curr = from_date
        while curr <= to_date:
            if curr.isoformat() not in activity_days and curr.weekday() < 5:
                inactivity_days.append(curr.isoformat())
            curr += timedelta(days=1)

    hour_with_most_activity = None
    if activity_by_hour:
        hour_with_most_activity = int(
            max(activity_by_hour, key=lambda h: activity_by_hour[h])
        )

    modifications_per_file = (
        round(total_modifications / total_files_handled, 2) if total_files_handled > 0 else 0
    )
    manual_percent = (
        round((total_manual / total_files_handled) * 100, 2) if total_files_handled > 0 else 0
    )
    auto_percent = (
        round((total_automatic / total_files_handled) * 100, 2) if total_files_handled > 0 else 0
    )

    return {
        "user": username,
        "daily_metrics": daily_metrics,
        "summary": {
            "total_manual_files": total_manual,
            "total_automatic_files": total_automatic,
            "total_files_handled": total_files_handled,
            "total_modifications": total_modifications,
            "avg_files_per_day": avg_files_per_day,
            # avg_creation_time: hours from user's first MODIFIED → file's WRT_ENT
            # e.g. 0.014 = 0.014h = ~50 seconds per file
            "avg_creation_time": avg_creation_time,
            "avg_creation_time_minutes": round(avg_creation_time * 60, 1) if avg_creation_time else None,
            "most_productive_day": most_productive_day,
            "file_type_counts": file_type_counts,
            "activity_by_hour": activity_by_hour,
            "company_specialization": company_spec,
            "principal_specialization": principal_spec,
            "days_active": days_active,
            "modifications_per_file": modifications_per_file,
            "manual_vs_auto_ratio": {
                "manual_percent": manual_percent,
                "automatic_percent": auto_percent,
            },
            "activity_days": activity_days,
            "inactivity_days": inactivity_days,
            "hour_with_most_activity": hour_with_most_activity,
            "total_deletions": total_deletions,
            "deleted_own_files": total_deleted_own,
            "deleted_others_files": total_deleted_others,
            "deleted_manual_files": total_deleted_manual,
            "deleted_automatic_files": total_deleted_automatic,
        },
    }


# ===========================================================================
# ROUTE 5 – POST /refresh
# Reads last 10 working days parquet files → writes 10-day summary cache.
# Only reads small daily files, not the entire history.
# ===========================================================================

def refresh_10day(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("refresh (10-day): loading last 10 working days parquet files")

    working_days = _last_n_working_days(10)

    frames = []
    for day in working_days:
        path = _daily_parquet_path(day)
        df_day = _read_parquet(path)
        if not df_day.empty:
            frames.append(df_day)

    if not frames:
        return func.HttpResponse(
            json.dumps({"status": "no_data", "message": "No parquet files found for the last 10 working days. Run /transform-daily first."}),
            status_code=200, mimetype="application/json"
        )

    df = pd.concat(frames, ignore_index=True)

    # Standardise
    for col in ["USERCODE", "HISTORY_STATUS", "ACTIVECOMPANY", "TYPEDECLARATIONSSW", PRINCIPAL_FIELD]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    df["HISTORYDATETIME"] = pd.to_datetime(df["HISTORYDATETIME"], errors="coerce", format="mixed")
    df = df.dropna(subset=["HISTORYDATETIME"])
    df["HISTORYDATETIME"] = df["HISTORYDATETIME"].dt.tz_localize(None)

    if "ACTIVECOMPANY" in df.columns:
        df = df[df["ACTIVECOMPANY"] != "DKM_VP"]

    # Auto-discover all unique users from the raw data
    all_user_codes = set(df["USERCODE"].dropna().unique())

    # Dedupe once globally, then pre-group by declaration so we never re-filter or
    # re-group the frame per user. The per-user crediting logic below is byte-
    # identical to before — we've only hoisted the shared groupby/dedup out of the
    # user loop (the part that made this O(users) and slow).
    df = df.drop_duplicates(
        subset=["DECLARATIONID", "USERCODE", "HISTORY_STATUS", "HISTORYDATETIME"]
    )
    decl_groups = {
        decl_id: g.sort_values("HISTORYDATETIME")
        for decl_id, g in df.groupby("DECLARATIONID")
    }
    user_to_decls = df.groupby("USERCODE")["DECLARATIONID"].unique()
    working_days_set = set(working_days)

    results = []
    for user in sorted(all_user_codes):
        user_daily = {day.strftime("%d/%m"): 0 for day in working_days}

        user_decls = user_to_decls.get(user, [])
        if len(user_decls) == 0:
            results.append({"user": user, "daily_file_creations": user_daily})
            continue

        for decl_id in user_decls:
            group = decl_groups[decl_id]
            user_rows = group[group["USERCODE"] == user]
            if user_rows.empty:
                continue

            first_action_date = user_rows["HISTORYDATETIME"].min().date()
            if first_action_date not in working_days_set:
                continue

            is_manual, is_automatic = classify_file_activity(
                global_history=group["HISTORY_STATUS"].tolist(),
                user_history=user_rows["HISTORY_STATUS"].tolist(),
                group_df=group,
                target_user=user,
                prefer_creation_status_owner=True
            )

            if is_manual or is_automatic:
                key = first_action_date.strftime("%d/%m")
                user_daily[key] += 1

        results.append({"user": user, "daily_file_creations": user_daily})

    _write_json_blob(results, SUMMARY_BLOB_PATH)

    return func.HttpResponse(
        json.dumps({"status": "success", "days_processed": len(frames), "users": len(results)}),
        status_code=200, mimetype="application/json"
    )


# ===========================================================================
# ROUTE 6 – GET /file-lifecycle
# Looks up a specific DECLARATIONID from the index + relevant daily parquet.
# ===========================================================================

def file_lifecycle(req: func.HttpRequest) -> func.HttpResponse:
    from performanceV3.functions.file_lifecycle import get_file_lifecycle

    declaration_id = req.params.get("id")
    if not declaration_id:
        return func.HttpResponse(
            json.dumps({"error": "Missing 'id' parameter"}),
            status_code=400, mimetype="application/json"
        )

    # Try to find the declaration in the index first to know which day parquet to load
    index_df = _read_parquet(INDEX_BLOB_PATH)
    if not index_df.empty:
        index_df["DECLARATIONID"] = index_df["DECLARATIONID"].astype(str)
        decl_id_str = str(int(float(declaration_id))).strip()
        match = index_df[index_df["DECLARATIONID"] == decl_id_str]
        if not match.empty:
            first_seen = match.iloc[0]["first_seen"]
            last_seen = match.iloc[0]["last_seen"]
            # Load parquets covering the lifecycle date range
            start_date = datetime.strptime(first_seen, "%Y-%m-%d").date()
            end_date = datetime.strptime(last_seen, "%Y-%m-%d").date()
            frames = []
            curr = start_date
            while curr <= end_date:
                path = _daily_parquet_path(curr)
                df_day = _read_parquet(path)
                if not df_day.empty:
                    frames.append(df_day)
                curr += timedelta(days=1)

            if frames:
                df_full = pd.concat(frames, ignore_index=True)
                result = get_file_lifecycle(df_full, declaration_id)
                return func.HttpResponse(json.dumps(result, default=str), status_code=200, mimetype="application/json")

    return func.HttpResponse(
        json.dumps({"found": False, "declaration_id": declaration_id, "message": "Declaration not found in index."}),
        status_code=404, mimetype="application/json"
    )


# ===========================================================================
# ROUTE – GET /users
# Returns all discovered human users with basic stats from the index.
# ===========================================================================

def list_users(req: func.HttpRequest) -> func.HttpResponse:
    """Return all human users found in the index with summary info."""
    logging.info("list-users: loading index to discover users")

    index_df = _read_parquet(INDEX_BLOB_PATH)
    if index_df.empty:
        return func.HttpResponse(
            json.dumps({"status": "no_data", "message": "Index is empty. Run /build-index first.", "users": []}),
            status_code=200, mimetype="application/json"
        )

    index_df["users"] = index_df["users"].apply(_safe_json_loads)

    # Optimized single-pass discovery of min/max dates for all users
    user_map = {}
    for _, row in index_df.iterrows():
        # index_df["users"] is already parsed as list via _safe_json_loads above
        user_list = row["users"]
        if not isinstance(user_list, list):
            continue
            
        fs = row["first_seen"]
        ls = row["last_seen"]
        
        for u in user_list:
            u_upper = u.upper().strip()
            if not u_upper or u_upper in SYSTEM_USERS or u_upper in ("NAN", "NONE", ""):
                continue
                
            if u_upper not in user_map:
                user_map[u_upper] = {
                    "usercode": u_upper,
                    "first_seen": fs,
                    "last_seen": ls
                }
            else:
                if fs and (not user_map[u_upper]["first_seen"] or fs < user_map[u_upper]["first_seen"]):
                    user_map[u_upper]["first_seen"] = fs
                if ls and (not user_map[u_upper]["last_seen"] or ls > user_map[u_upper]["last_seen"]):
                    user_map[u_upper]["last_seen"] = ls

    user_details = sorted(user_map.values(), key=lambda x: x["usercode"])

    return func.HttpResponse(
        json.dumps({
            "status": "success",
            "total_users": len(user_details),
            "users": user_details,
        }, default=str),
        status_code=200, mimetype="application/json"
    )


# ===========================================================================
# MAIN ENTRY POINT
# ===========================================================================

def main(req: func.HttpRequest) -> func.HttpResponse:
    if not connection_string:
        return func.HttpResponse(
            json.dumps({"error": "Backend service not configured."}),
            status_code=503, mimetype="application/json"
        )

    try:
        method = req.method
        action = req.route_params.get("action")
        user_param = req.params.get("user")

        # ---------------------------------------------------------------
        # POST routes (heavy computation / transformation)
        # ---------------------------------------------------------------

        if method == "POST" and action == "transform-daily":
            # Convert raw landing JSON files → daily parquet (single day)
            return transform_daily(req)

        elif method == "POST" and action == "transform-daily-range":
            # 📦 Batch backfill: process all days from ?start= to ?end= in one call
            return transform_daily_range(req)

        elif method == "POST" and action == "build-index":
            # Build/rebuild file_index.parquet from all daily parquets
            return build_index(req)

        elif method == "POST" and action == "refresh-users":
            # Build per-user JSON caches in a single pass over all daily parquets
            return refresh_users(req)

        elif method == "POST" and action == "refresh":
            # Full rebuild of 10-day summary cache (daily cold run)
            return refresh_10day(req)

        # ---------------------------------------------------------------
        # GET routes (instant reads from cache / index)
        # ---------------------------------------------------------------

        elif method == "GET" and action == "users":
            # 👥 List all discovered users with basic stats
            return list_users(req)

        elif method == "GET" and action == "file-lifecycle":
            return file_lifecycle(req)

        elif method == "GET" and action == "debug-index":
            # Show a sample of the index for debugging
            index_df = _read_parquet(INDEX_BLOB_PATH)
            if index_df.empty:
                return func.HttpResponse(
                    json.dumps({"message": "Index is empty. Run /build-index first."}),
                    status_code=200, mimetype="application/json"
                )
            sample = index_df.head(20).to_dict(orient="records")
            return func.HttpResponse(
                json.dumps({
                    "total_rows": len(index_df),
                    "columns": list(index_df.columns),
                    "sample": sample
                }, default=str),
                status_code=200, mimetype="application/json"
            )

        elif method == "GET" and action == "debug-landing":
            # List all landing JSON blobs
            blobs = _list_blobs(LANDING_PREFIX)
            return func.HttpResponse(
                json.dumps({"landing_blobs": blobs, "count": len(blobs)}),
                status_code=200, mimetype="application/json"
            )

        elif method == "GET" and action == "debug-transformed":
            # List all daily parquet blobs
            blobs = _list_blobs(TRANSFORMED_PREFIX)
            return func.HttpResponse(
                json.dumps({"transformed_blobs": [b for b in blobs if b.endswith(".parquet")], "count": len(blobs)}),
                status_code=200, mimetype="application/json"
            )

        elif method == "GET" and user_param:
            # Instant: read pre-computed user cache
            user_blob_path = f"{USER_CACHE_PATH_PREFIX}{user_param}.json"
            logging.info(f"Reading user cache: {user_blob_path}")
            bc = _blob_client(user_blob_path)
            if not bc.exists():
                return func.HttpResponse(
                    json.dumps({"error": f"Cache for user '{user_param}' not found. Trigger POST /refresh-users first."}),
                    status_code=404, mimetype="application/json"
                )
            return func.HttpResponse(bc.download_blob().readall(), status_code=200, mimetype="application/json")

        elif method == "GET" and not action:
            # Instant: read 10-day summary cache
            logging.info(f"Reading 10-day summary cache: {SUMMARY_BLOB_PATH}")
            bc = _blob_client(SUMMARY_BLOB_PATH)
            if not bc.exists():
                return func.HttpResponse(
                    json.dumps({"error": "10-day summary cache not found. Trigger POST /refresh first."}),
                    status_code=404, mimetype="application/json"
                )
            return func.HttpResponse(bc.download_blob().readall(), status_code=200, mimetype="application/json")

        else:
            return func.HttpResponse(
                json.dumps({"error": "Endpoint not found or method not allowed.", "method": method, "action": action}),
                status_code=404, mimetype="application/json"
            )

    except Exception as e:
        logging.error(f"Unexpected error in performanceV3: {e}", exc_info=True)
        return func.HttpResponse(
            json.dumps({"error": "An internal server error occurred.", "detail": str(e)}),
            status_code=500, mimetype="application/json"
        )
