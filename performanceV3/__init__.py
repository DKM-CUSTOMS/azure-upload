"""
performanceV3 - Azure Function
==============================
New data architecture that eliminates memory crashes on Azure consumption plan.

╔══════════════════════════════════════════════════════════════════════════════╗
║  LIVE (every 2 hours) — triggered by Logic App after blob upload            ║
║    POST /refresh-hot                                                        ║
║      1. Read only the NEW 2h JSON landing file (sent as body or auto-detect)║
║      2. Merge into today's daily parquet (upsert by DECLARATIONID)          ║
║      3. Update file index — only for declarations in new data               ║
║      4. Rebuild 10-day summary cache (10 small daily parquets ~10 MB each)  ║
║      5. Rebuild user caches — only for users that appear in the new data    ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  DAILY (2 AM cold rebuild)                                                  ║
║    POST /transform-daily → /build-index → /refresh-monthly → /refresh      ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  INSTANT GET endpoints (always read from cache — no calculation)            ║
║    GET /           → 10-day summary cache                                   ║
║    GET ?user=X     → per-user cache                                         ║
║    GET ?all_users  → monthly report cache                                   ║
╚══════════════════════════════════════════════════════════════════════════════╝

Memory budget per refresh-hot call:
  New 2h JSON      : ~5–20 MB
  Today's parquet  : ~10–50 MB   (read + re-write)
  File index       : ~50 MB      (read + patch + re-write)
  10-day parquets  : ~10x10 MB   (read only)
  User cache write : ~1 MB each  (only affected users)
  TOTAL            : ~200 MB     ✅ safe on consumption plan
"""

from collections import defaultdict
from datetime import datetime, timedelta
import azure.functions as func
import logging
import json
import io
import pandas as pd
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
MONTHLY_SUMMARY_BLOB_PATH = f"Dashboard/cache/monthly_report_cacheV3.json"
USER_CACHE_PATH_PREFIX = "Dashboard/cache/usersV3/"

# Import declaration types — used to determine team membership
IMPORT_TYPES = {"DMS_IMPORT", "IDMS_IMPORT"}

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
    """Read a single parquet file from blob storage into a DataFrame."""
    bc = _blob_client(path)
    if not bc.exists():
        logging.warning(f"Parquet not found: {path}")
        return pd.DataFrame()
    data = bc.download_blob().readall()
    return pd.read_parquet(io.BytesIO(data), columns=columns)


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

    frames = []
    for blob_name in json_blobs:
        try:
            raw = _blob_client(blob_name).download_blob().readall()
            records = json.loads(raw)
            if isinstance(records, list) and records:
                frames.append(pd.DataFrame(records))
        except Exception as e:
            logging.error(f"Failed to read blob {blob_name}: {e}")

    if not frames:
        return func.HttpResponse(
            json.dumps({"status": "no_data", "message": f"All JSON files for {target_date} were empty or unreadable."}),
            status_code=200, mimetype="application/json"
        )

    df = pd.concat(frames, ignore_index=True)

    # Standardise columns
    for col in ["USERCODE", "HISTORY_STATUS", "ACTIVECOMPANY", "TYPEDECLARATIONSSW"]:
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

    frames = []
    for blob_name in json_blobs:
        try:
            raw = _blob_client(blob_name).download_blob().readall()
            records = json.loads(raw)
            if isinstance(records, list) and records:
                frames.append(pd.DataFrame(records))
        except Exception as e:
            logging.error(f"transform-range: failed to read {blob_name}: {e}")

    if not frames:
        return {"date": date_str, "status": "no_data", "reason": "all JSON files were empty or unreadable"}

    df = pd.concat(frames, ignore_index=True)
    for col in ["USERCODE", "HISTORY_STATUS", "ACTIVECOMPANY", "TYPEDECLARATIONSSW"]:
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
CUSTOMSILS_FIELD = "CUSTOMSILS"    # principal field (may be absent from source)

def build_index(req: func.HttpRequest) -> func.HttpResponse:
    """
    Reads all transformed/year=.../month=.../day=.../data.parquet files,
    groups by DECLARATIONID, and writes index/file_index.parquet.
    """
    logging.info("build-index: starting full scan of transformed parquet files")

    all_parquet_blobs = _list_blobs(TRANSFORMED_PREFIX)
    parquet_blobs = [b for b in all_parquet_blobs if b.endswith(".parquet")]

    if not parquet_blobs:
        return func.HttpResponse(
            json.dumps({"status": "no_data", "message": "No transformed parquet files found."}),
            status_code=200, mimetype="application/json"
        )

    logging.info(f"build-index: loading {len(parquet_blobs)} parquet files")

    frames = []
    for blob_name in parquet_blobs:
        try:
            frames.append(_read_parquet(blob_name))
        except Exception as e:
            logging.error(f"Failed to read parquet {blob_name}: {e}")

    if not frames:
        return func.HttpResponse(
            json.dumps({"error": "Could not read any transformed parquet files."}),
            status_code=500, mimetype="application/json"
        )

    df = pd.concat(frames, ignore_index=True)

    # Standardise
    for col in ["USERCODE", "HISTORY_STATUS", "ACTIVECOMPANY", "TYPEDECLARATIONSSW"]:
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
        principal = str(group[CUSTOMSILS_FIELD].iloc[0]) if CUSTOMSILS_FIELD in group.columns else ""
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
# Strategy: index is used ONLY to find which declarations each user is in
# and their date range. Then we load daily parquets ONE AT A TIME, filter
# immediately to only that user's declarations, and discard each full parquet.
# At any moment we hold at most one daily parquet (~10-20 MB) in memory.
# The resulting per-user DataFrame is small (~few MB) and we compute the
# full rich metrics from it — matching the exact V2 output format.
# ===========================================================================

def _discover_all_users(index_df: pd.DataFrame) -> list:
    """
    Extract all unique human USERCODE values from the index.
    Returns a sorted list of usernames, excluding system users.
    """
    all_users = set()
    for user_list in index_df["users"]:
        if isinstance(user_list, list):
            for u in user_list:
                u_upper = u.upper().strip()
                if u_upper and u_upper not in SYSTEM_USERS and u_upper not in ("NAN", "NONE", ""):
                    all_users.add(u_upper)
    return sorted(all_users)


def refresh_users(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("refresh-users: starting rich user metrics rebuild")

    index_df = _read_parquet(INDEX_BLOB_PATH)
    if index_df.empty:
        return func.HttpResponse(
            json.dumps({"status": "skipped", "message": "Index is empty. Run /build-index first."}),
            status_code=200, mimetype="application/json"
        )

    # Parse users list from index
    index_df["users"] = index_df["users"].apply(_safe_json_loads)
    index_df["DECLARATIONID"] = index_df["DECLARATIONID"].astype(str)

    # Auto-discover all human users from the index
    all_users = _discover_all_users(index_df)
    logging.info(f"refresh-users: discovered {len(all_users)} human users from the index")

    processed_count = 0
    failed_count = 0

    for user in all_users:
        username_upper = user.upper()
        try:
            # Step A: find this user's declarations and date range from the index
            user_index_rows = index_df[index_df["users"].apply(
                lambda us: username_upper in [u.upper() for u in (us if isinstance(us, list) else [])]
            )]

            if user_index_rows.empty:
                logging.info(f"refresh-users: no data found for {user}")
                _write_json_blob(
                    {"user": user, "daily_metrics": [], "summary": {}},
                    f"{USER_CACHE_PATH_PREFIX}{user}.json"
                )
                continue

            decl_ids = set(user_index_rows["DECLARATIONID"].tolist())
            first_date = datetime.strptime(user_index_rows["first_seen"].min(), "%Y-%m-%d").date()
            last_date = datetime.strptime(user_index_rows["last_seen"].max(), "%Y-%m-%d").date()

            logging.info(f"refresh-users: {user} → {len(decl_ids)} declarations, {first_date} → {last_date}")

            # Step B: load daily parquets for date range, filter immediately per parquet
            filtered_frames = []
            curr = first_date
            while curr <= last_date:
                path = _daily_parquet_path(curr)
                df_day = _read_parquet(path)
                if not df_day.empty:
                    df_day["DECLARATIONID"] = df_day["DECLARATIONID"].astype(str)
                    filtered = df_day[df_day["DECLARATIONID"].isin(decl_ids)]
                    if not filtered.empty:
                        filtered_frames.append(filtered)
                    del df_day  # free memory immediately
                curr += timedelta(days=1)

            if not filtered_frames:
                logging.warning(f"refresh-users: {user} — no parquet data found in date range")
                _write_json_blob(
                    {"user": user, "daily_metrics": [], "summary": {}},
                    f"{USER_CACHE_PATH_PREFIX}{user}.json"
                )
                continue

            # Step C: combine the small filtered frames and compute rich metrics
            user_df = pd.concat(filtered_frames, ignore_index=True)
            del filtered_frames  # free memory

            metrics = _compute_rich_user_metrics(user_df, user)
            del user_df  # free memory

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
    for col in ["USERCODE", "HISTORY_STATUS", "ACTIVECOMPANY", "TYPEDECLARATIONSSW", "CUSTOMSILS"]:
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
    # CUSTOMSILS = the principal/client identifier on the declaration
    principal_spec = {}
    if "CUSTOMSILS" in df.columns and not user_actions.empty:
        principal_counts = user_actions["CUSTOMSILS"].value_counts().to_dict()
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
    })

    total_manual = 0
    total_automatic = 0
    total_modifications = 0
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
            target_user=username_upper
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
        },
    }


# Keep the index-based version for the hot path (fast, less detail)
def _calculate_user_metrics_from_index(index_df: pd.DataFrame, username: str) -> dict:
    """
    Lightweight version using only the pre-built index.
    Used by refresh-hot for fast incremental updates.
    For full rich metrics, see _compute_rich_user_metrics().
    """
    username_upper = username.upper()
    user_rows = index_df[index_df["users"].apply(
        lambda us: username_upper in [u.upper() for u in (us if isinstance(us, list) else [])]
    )]
    if user_rows.empty:
        return {"user": username, "summary": {}, "monthly_breakdown": []}

    system_users = {"BATCHPROC", "ADMIN", "SYSTEM", "BATCH_PROC"}
    is_creator = user_rows["created_by"] == username_upper
    is_modifier = user_rows["modified_by"] == username_upper
    sys_created = user_rows["created_by"].isin(system_users)
    
    gets_credit = (is_creator & ~sys_created) | (sys_created & is_modifier)
    credited_rows = user_rows[gets_credit].copy()

    is_auto_file = credited_rows["has_interface"] | credited_rows["created_by"].isin(system_users)
    is_manual_file = credited_rows["has_manual_trigger"] & ~is_auto_file

    total_manual = int(is_manual_file.sum())
    total_auto = int(is_auto_file.sum())
    total_files = total_manual + total_auto
    durations = credited_rows["file_creation_duration"].dropna().tolist()
    avg_duration = round(sum(durations) / len(durations), 2) if durations else None
    
    total_mods = int(user_rows["modification_count"].sum())

    credited_rows["month"] = pd.to_datetime(
        credited_rows["first_seen"], errors="coerce"
    ).dt.to_period("M").astype(str)
    monthly = []
    for month_str, grp in credited_rows.groupby("month"):
        grp_auto = grp["has_interface"] | grp["created_by"].isin(system_users)
        grp_manual = grp["has_manual_trigger"] & ~grp_auto
        manual_files = int(grp_manual.sum())
        automatic_files = int(grp_auto.sum())
        monthly.append({
            "month": month_str,
            "total_files": manual_files + automatic_files,
            "manual_files": manual_files,
            "automatic_files": automatic_files,
            "total_modifications": int(grp["modification_count"].sum()),
        })

    return {
        "user": username,
        "summary": {
            "total_manual_files": total_manual,
            "total_automatic_files": total_auto,
            "total_files_handled": total_files,
            "total_modifications": total_mods,
            "avg_file_creation_duration_hours": avg_duration,
        },
        "monthly_breakdown": sorted(monthly, key=lambda x: x["month"])
    }


# ===========================================================================
# ROUTE 4 – POST /refresh-monthly
# Reads the index → writes monthly report cache (all discovered users in ~30 days).
# ===========================================================================

def refresh_monthly(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("refresh-monthly: loading file index")

    index_df = _read_parquet(INDEX_BLOB_PATH)
    if index_df.empty:
        return func.HttpResponse(
            json.dumps({"status": "skipped", "message": "Index is empty. Run /build-index first."}),
            status_code=200, mimetype="application/json"
        )

    index_df["users"] = index_df["users"].apply(_safe_json_loads)

    cutoff = (datetime.utcnow() - timedelta(days=30)).date().isoformat()
    recent = index_df[index_df["first_seen"] >= cutoff]

    all_users = _discover_all_users(index_df)
    logging.info(f"refresh-monthly: discovered {len(all_users)} users")

    results = []
    for username in all_users:
        username_upper = username.upper()

        user_rows = recent[recent["users"].apply(
            lambda us: username_upper in [u.upper() for u in (us if isinstance(us, list) else [])]
        )]

        if user_rows.empty:
            continue

        system_users = {"BATCHPROC", "ADMIN", "SYSTEM", "BATCH_PROC"}
        is_creator = user_rows["created_by"] == username_upper
        is_modifier = user_rows["modified_by"] == username_upper
        sys_created = user_rows["created_by"].isin(system_users)
        
        gets_credit = (is_creator & ~sys_created) | (sys_created & is_modifier)
        credited_rows = user_rows[gets_credit]

        is_auto_file = credited_rows["has_interface"] | credited_rows["created_by"].isin(system_users)
        is_manual_file = credited_rows["has_manual_trigger"] & ~is_auto_file

        total_manual = int(is_manual_file.sum())
        total_auto = int(is_auto_file.sum())
        total_creations = total_manual + total_auto
        total_sent = int(credited_rows["sending_count"].sum())

        # Working days with activity (unique weekdays)
        dates = pd.to_datetime(credited_rows["first_seen"], errors="coerce").dt.date
        working_days_active = len(set(d for d in dates if pd.notna(d) and d.weekday() < 5))

        avg_per_day = round(total_creations / working_days_active, 2) if working_days_active > 0 else 0

        results.append({
            "user": username,
            "total_files_handled": total_creations,
            "manual_files": total_manual,
            "automatic_files": total_auto,
            "sent_files": total_sent,
            "days_with_activity": working_days_active,
            "avg_activity_per_day": avg_per_day,
            "manual_vs_auto_ratio": {
                "manual_percent": round((total_manual / total_creations) * 100, 2) if total_creations else 0,
                "automatic_percent": round((total_auto / total_creations) * 100, 2) if total_creations else 0,
            }
        })

    _write_json_blob(results, MONTHLY_SUMMARY_BLOB_PATH)

    return func.HttpResponse(
        json.dumps({"status": "success", "users_in_report": len(results)}),
        status_code=200, mimetype="application/json"
    )


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
    for col in ["USERCODE", "HISTORY_STATUS", "ACTIVECOMPANY", "TYPEDECLARATIONSSW"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    df["HISTORYDATETIME"] = pd.to_datetime(df["HISTORYDATETIME"], errors="coerce", format="mixed")
    df = df.dropna(subset=["HISTORYDATETIME"])
    df["HISTORYDATETIME"] = df["HISTORYDATETIME"].dt.tz_localize(None)

    if "ACTIVECOMPANY" in df.columns:
        df = df[df["ACTIVECOMPANY"] != "DKM_VP"]

    # Auto-discover all unique users from the raw data
    all_user_codes = set(df["USERCODE"].dropna().unique())

    results = []
    for user in sorted(all_user_codes):
        user_daily = {day.strftime("%d/%m"): 0 for day in working_days}

        user_decls = df[df["USERCODE"] == user]["DECLARATIONID"].unique()
        if len(user_decls) == 0:
            results.append({"user": user, "daily_file_creations": user_daily})
            continue

        user_scope_df = df[df["DECLARATIONID"].isin(user_decls)].copy()
        user_scope_df = user_scope_df.drop_duplicates(
            subset=["DECLARATIONID", "USERCODE", "HISTORY_STATUS", "HISTORYDATETIME"]
        )

        for decl_id, group in user_scope_df.groupby("DECLARATIONID"):
            group = group.sort_values("HISTORYDATETIME")
            user_rows = group[group["USERCODE"] == user]
            if user_rows.empty:
                continue

            first_action_date = user_rows["HISTORYDATETIME"].min().date()
            if first_action_date not in working_days:
                continue

            is_manual, is_automatic = classify_file_activity(
                global_history=group["HISTORY_STATUS"].tolist(),
                user_history=user_rows["HISTORY_STATUS"].tolist(),
                group_df=group,
                target_user=user
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
# ROUTE 7 – POST /refresh-hot
# ═══════════════════════════════════════════════════════════════════════════
# This is the LIVE pipeline. Call it from your Logic App immediately after
# saving the JSON blob. It processes only the NEW 2-hour data slice and
# does an incremental update of the index + caches.
#
# How to call it from Logic App:
#   Add an HTTP action AFTER "Create_blob_(V2)" step:
#     POST https://<your-function-app>.azurewebsites.net/api/performanceV3/refresh-hot
#     Headers: x-functions-key: <your-key>
#     Body: { "blob_name": "<full path of the blob just created>" }
#
# Alternatively (auto-detect mode, no body needed):
#   It will find all landing JSON blobs written in the last 3 hours that
#   have NOT yet been merged into today's parquet.
# ===========================================================================

# Tracks which landing blobs have been processed (stored in blob storage)
HOT_STATE_BLOB = f"{BLOB_BASE}/state/hot_processed.json"


def _load_hot_state() -> set:
    """Load the set of already-processed landing blob names."""
    raw = _read_json_blob(HOT_STATE_BLOB)
    if raw and isinstance(raw, list):
        return set(raw)
    return set()


def _save_hot_state(processed: set):
    """Persist the set of processed blob names (keep last 500 to avoid unbounded growth)."""
    ordered = sorted(processed)[-500:]  # keep only the most recent 500
    _write_json_blob(ordered, HOT_STATE_BLOB)


def _build_index_rows_for_df(df: pd.DataFrame) -> list:
    """
    Given a DataFrame of raw history rows, return a list of index-row dicts
    (one per DECLARATIONID).  Same logic as build_index() but works on a
    small subset of data so it runs in milliseconds.
    """
    system_users = {"BATCHPROC", "ADMIN", "SYSTEM", "BATCH_PROC"}
    rows = []

    for decl_id, group in df.groupby("DECLARATIONID"):
        group = group.sort_values("HISTORYDATETIME")
        statuses = group["HISTORY_STATUS"].tolist()
        has_interface = "INTERFACE" in set(statuses)
        has_manual_trigger = bool({"COPIED", "COPY", "NEW"}.intersection(set(statuses)))
        first_seen = group["HISTORYDATETIME"].min().date().isoformat()
        last_seen = group["HISTORYDATETIME"].max().date().isoformat()

        company = str(group["ACTIVECOMPANY"].iloc[0]) if "ACTIVECOMPANY" in group.columns else ""
        principal = str(group[CUSTOMSILS_FIELD].iloc[0]) if CUSTOMSILS_FIELD in group.columns else ""
        type_val = str(group["TYPEDECLARATIONSSW"].iloc[0]) if "TYPEDECLARATIONSSW" in group.columns else ""

        unique_users = list(group["USERCODE"].unique())
        human_users = [u for u in unique_users if u not in system_users]
        created_by = str(group.iloc[0]["USERCODE"])

        human_actions = group[~group["USERCODE"].isin(system_users)]
        mod_rows = human_actions[human_actions["HISTORY_STATUS"] == "MODIFIED"]
        if not mod_rows.empty:
            modified_by = str(mod_rows["USERCODE"].value_counts().idxmax())
        elif not human_actions.empty:
            modified_by = str(human_actions.iloc[0]["USERCODE"])
        else:
            modified_by = created_by

        session_start = None
        file_creation_duration = None
        for _, row in group.iterrows():
            if row["HISTORY_STATUS"] == "MODIFIED" and session_start is None:
                session_start = row["HISTORYDATETIME"]
            elif row["HISTORY_STATUS"] == "WRT_ENT" and session_start is not None:
                file_creation_duration = round((row["HISTORYDATETIME"] - session_start).total_seconds() / 3600, 3)
                break

        rows.append({
            "DECLARATIONID": decl_id,
            "first_seen": first_seen,
            "last_seen": last_seen,
            "principal": principal,
            "company": company,
            "users": json.dumps(unique_users),
            "human_users": json.dumps(human_users),
            "has_interface": has_interface,
            "has_manual_trigger": has_manual_trigger,
            "created_by": created_by,
            "modified_by": modified_by,
            "file_creation_duration": file_creation_duration,
            "statuses": json.dumps(list(set(statuses))),
            "sending_count": int((group["HISTORY_STATUS"] == "DEC_DAT").sum()),
            "modification_count": int((group["HISTORY_STATUS"] == "MODIFIED").sum()),
            "type": type_val,
        })
    return rows


def refresh_hot(req: func.HttpRequest) -> func.HttpResponse:
    """
    Near-live pipeline. Called by Logic App every 2 hours after a new JSON
    blob is uploaded to the landing zone.

    Steps:
      1. Identify which landing blobs are NEW (not yet processed)
      2. Parse the new JSON blob(s) → small DataFrame
      3. Merge into today's daily parquet (upsert on DECLARATIONID)
      4. Patch the file index — only rows for declarations in new data
      5. Rebuild 10-day summary cache
      6. Rebuild user caches — only users appearing in new data
      7. Save updated hot-state so we never re-process the same blob
    """
    logging.info("=== refresh-hot started ===")
    now_utc = datetime.utcnow()
    today = now_utc.date()

    # force=true skips the already-processed check (useful for testing)
    force = req.params.get("force", "false").lower() == "true"

    # ------------------------------------------------------------------
    # STEP 1 – identify which blob(s) to process
    # ------------------------------------------------------------------
    # Prefer explicit blob_name from request body (sent by Logic App)
    new_blob_names = []
    try:
        body = req.get_json()
        explicit_blob = body.get("blob_name") if body else None
    except Exception:
        explicit_blob = None

    processed_set = _load_hot_state()

    if explicit_blob:
        # Logic App told us exactly which blob was just created
        if explicit_blob in processed_set and not force:
            return func.HttpResponse(
                json.dumps({"status": "skipped", "reason": f"Blob '{explicit_blob}' already processed. Add ?force=true to reprocess."}),
                status_code=200, mimetype="application/json"
            )
        new_blob_names = [explicit_blob]
    else:
        # Auto-detect: scan landing blobs from today and yesterday, find unprocessed
        for scan_date in [today, today - timedelta(days=1)]:
            prefix = f"{LANDING_PREFIX}day={scan_date.isoformat()}/"
            all_today_blobs = _list_blobs(prefix)
            new_blob_names += [b for b in all_today_blobs if b not in processed_set]

    if not new_blob_names:
        return func.HttpResponse(
            json.dumps({"status": "no_new_data", "message": "No new landing blobs to process."}),
            status_code=200, mimetype="application/json"
        )

    logging.info(f"refresh-hot: processing {len(new_blob_names)} new blob(s): {new_blob_names}")

    # ------------------------------------------------------------------
    # STEP 2 – parse the new blobs → DataFrame
    # ------------------------------------------------------------------
    frames = []
    for blob_name in new_blob_names:
        try:
            raw = _blob_client(blob_name).download_blob().readall()
            records = json.loads(raw)
            if isinstance(records, list) and records:
                frames.append(pd.DataFrame(records))
        except Exception as e:
            logging.error(f"refresh-hot: failed to read blob '{blob_name}': {e}")

    if not frames:
        return func.HttpResponse(
            json.dumps({"status": "no_data", "message": "New blobs were empty or unreadable."}),
            status_code=200, mimetype="application/json"
        )

    new_df = pd.concat(frames, ignore_index=True)

    # Standardise
    for col in ["USERCODE", "HISTORY_STATUS", "ACTIVECOMPANY", "TYPEDECLARATIONSSW"]:
        if col in new_df.columns:
            new_df[col] = new_df[col].astype(str).str.strip().str.upper()
    new_df["HISTORYDATETIME"] = pd.to_datetime(new_df["HISTORYDATETIME"], errors="coerce", format="mixed")
    new_df = new_df.dropna(subset=["HISTORYDATETIME", "DECLARATIONID"])
    if "ACTIVECOMPANY" in new_df.columns:
        new_df = new_df[new_df["ACTIVECOMPANY"] != "DKM_VP"]

    new_decl_ids = set(new_df["DECLARATIONID"].unique())
    affected_users = set(new_df["USERCODE"].dropna().unique()) - SYSTEM_USERS

    logging.info(f"refresh-hot: {len(new_df)} rows, {len(new_decl_ids)} declarations, "
                 f"{len(affected_users)} users affected")

    # ------------------------------------------------------------------
    # STEP 3 – merge into today's daily parquet (upsert)
    # ------------------------------------------------------------------
    today_parquet_path = _daily_parquet_path(today)
    existing_today = _read_parquet(today_parquet_path)

    if existing_today.empty:
        merged_today = new_df
    else:
        # Remove old rows for declarations that appear in the new data
        # (they may have new status rows we want to add)
        existing_other = existing_today[
            ~existing_today["DECLARATIONID"].isin(new_decl_ids)
        ]
        # Also keep old rows from the SAME declarations so we have the full history
        existing_same = existing_today[
            existing_today["DECLARATIONID"].isin(new_decl_ids)
        ]
        merged_today = pd.concat([existing_same, new_df, existing_other], ignore_index=True)

    # Deduplicate
    dup_cols = [c for c in ["DECLARATIONID", "USERCODE", "HISTORY_STATUS", "HISTORYDATETIME"] if c in merged_today.columns]
    merged_today = merged_today.drop_duplicates(subset=dup_cols)
    _write_parquet(merged_today, today_parquet_path)
    logging.info(f"refresh-hot: today's parquet updated → {len(merged_today)} rows")

    # ------------------------------------------------------------------
    # STEP 4 – patch the file index (incremental update)
    # ------------------------------------------------------------------
    index_df = _read_parquet(INDEX_BLOB_PATH)

    # Build a full picture for the affected declarations:
    # combine new data with their existing history in today's parquet
    affected_full_df = merged_today[merged_today["DECLARATIONID"].isin(new_decl_ids)].copy()

    # Also pull their history from past day parquets if they existed before today
    # We only do this if they're NOT brand-new declarations
    if not index_df.empty:
        index_df["DECLARATIONID"] = index_df["DECLARATIONID"].astype(str)
        new_decl_ids_str = {str(d) for d in new_decl_ids}
        existing_in_index = set(index_df["DECLARATIONID"].tolist())
        old_decls = new_decl_ids_str.intersection(existing_in_index)

        if old_decls:
            # Load their earliest history from parquet archive
            old_index_rows = index_df[index_df["DECLARATIONID"].isin(old_decls)]
            min_date_str = old_index_rows["first_seen"].min()
            try:
                scan_start = datetime.strptime(min_date_str, "%Y-%m-%d").date()
            except Exception:
                scan_start = today

            extra_frames = []
            curr = scan_start
            while curr < today:  # today is already in merged_today
                path = _daily_parquet_path(curr)
                df_d = _read_parquet(path)
                if not df_d.empty:
                    relevant = df_d[df_d["DECLARATIONID"].isin(old_decls)]
                    if not relevant.empty:
                        extra_frames.append(relevant)
                curr += timedelta(days=1)

            if extra_frames:
                historical = pd.concat(extra_frames, ignore_index=True)
                affected_full_df = pd.concat([historical, affected_full_df], ignore_index=True)
                affected_full_df = affected_full_df.drop_duplicates(
                    subset=[c for c in dup_cols if c in affected_full_df.columns]
                )

    # Compute updated index rows for affected declarations
    new_index_rows = _build_index_rows_for_df(affected_full_df)

    if not index_df.empty:
        # Remove old index entries for affected declarations
        index_df["DECLARATIONID"] = index_df["DECLARATIONID"].astype(str)
        affected_ids_str = {str(d) for d in new_decl_ids}
        index_df = index_df[~index_df["DECLARATIONID"].isin(affected_ids_str)]

    patch_df = pd.DataFrame(new_index_rows)
    updated_index_df = pd.concat([index_df, patch_df], ignore_index=True)
    _write_parquet(updated_index_df, INDEX_BLOB_PATH)
    logging.info(f"refresh-hot: index patched → {len(updated_index_df)} total declarations")

    # ------------------------------------------------------------------
    # STEP 5 – rebuild 10-day summary cache
    # ------------------------------------------------------------------
    working_days = _last_n_working_days(10)
    day_frames = []
    for day in working_days:
        path = _daily_parquet_path(day)
        df_day = _read_parquet(path)
        if not df_day.empty:
            day_frames.append(df_day)

    if day_frames:
        df_10 = pd.concat(day_frames, ignore_index=True)
        for col in ["USERCODE", "HISTORY_STATUS", "ACTIVECOMPANY", "TYPEDECLARATIONSSW"]:
            if col in df_10.columns:
                df_10[col] = df_10[col].astype(str).str.strip().str.upper()
        df_10["HISTORYDATETIME"] = pd.to_datetime(df_10["HISTORYDATETIME"], errors="coerce", format="mixed")
        df_10 = df_10.dropna(subset=["HISTORYDATETIME"])
        df_10["HISTORYDATETIME"] = df_10["HISTORYDATETIME"].dt.tz_localize(None)
        if "ACTIVECOMPANY" in df_10.columns:
            df_10 = df_10[df_10["ACTIVECOMPANY"] != "DKM_VP"]

        results_10 = []
        # Auto-discover users from the 10-day data
        all_10d_users = sorted(set(df_10["USERCODE"].dropna().unique()))

        for user in all_10d_users:
            user_daily = {day.strftime("%d/%m"): 0 for day in working_days}
            
            user_decls = df_10[df_10["USERCODE"] == user]["DECLARATIONID"].unique()
            if len(user_decls) == 0:
                results_10.append({"user": user, "daily_file_creations": user_daily})
                continue

            scope2 = df_10[df_10["DECLARATIONID"].isin(user_decls)].copy()
            scope2 = scope2.drop_duplicates(
                subset=[c for c in ["DECLARATIONID", "USERCODE", "HISTORY_STATUS", "HISTORYDATETIME"] if c in scope2.columns]
            )
            for decl_id, group in scope2.groupby("DECLARATIONID"):
                group = group.sort_values("HISTORYDATETIME")
                user_rows = group[group["USERCODE"] == user]
                if user_rows.empty:
                    continue
                first_action_date = user_rows["HISTORYDATETIME"].min().date()
                if first_action_date not in working_days:
                    continue
                is_manual, is_automatic = classify_file_activity(
                    global_history=group["HISTORY_STATUS"].tolist(),
                    user_history=user_rows["HISTORY_STATUS"].tolist(),
                    group_df=group,
                    target_user=user
                )
                if is_manual or is_automatic:
                    user_daily[first_action_date.strftime("%d/%m")] += 1

            results_10.append({"user": user, "daily_file_creations": user_daily})

        _write_json_blob(results_10, SUMMARY_BLOB_PATH)
        logging.info("refresh-hot: 10-day summary cache updated")

    # ------------------------------------------------------------------
    # STEP 6 – refresh user caches for affected users only
    # ------------------------------------------------------------------
    updated_index_df["users"] = updated_index_df["users"].apply(_safe_json_loads)
    updated_index_df["statuses"] = updated_index_df["statuses"].apply(_safe_json_loads)

    user_cache_count = 0
    for user in affected_users:
        try:
            metrics = _calculate_user_metrics_from_index(updated_index_df, user)
            _write_json_blob(metrics, f"{USER_CACHE_PATH_PREFIX}{user}.json")
            user_cache_count += 1
        except Exception as e:
            logging.error(f"refresh-hot: failed to update cache for {user}: {e}")

    logging.info(f"refresh-hot: updated {user_cache_count} user caches")

    # ------------------------------------------------------------------
    # STEP 7 – mark blobs as processed
    # ------------------------------------------------------------------
    processed_set.update(new_blob_names)
    _save_hot_state(processed_set)

    return func.HttpResponse(
        json.dumps({
            "status": "success",
            "blobs_processed": new_blob_names,
            "new_rows": len(new_df),
            "declarations_updated": len(new_decl_ids),
            "user_caches_refreshed": user_cache_count,
            "affected_users": list(affected_users),
            "last_updated_utc": now_utc.isoformat()
        }),
        status_code=200, mimetype="application/json"
    )


# ===========================================================================
# ROUTE 8 – DELETE /reset-hot-state
# Clear the hot_processed.json so that already-processed blobs can be
# re-processed by refresh-hot. Use this during testing.
# ===========================================================================

def reset_hot_state() -> func.HttpResponse:
    """Wipe the hot-state file so refresh-hot treats all blobs as new."""
    bc = _blob_client(HOT_STATE_BLOB)
    existed = bc.exists()
    if existed:
        bc.delete_blob()
    _write_json_blob([], HOT_STATE_BLOB)   # write empty list back
    return func.HttpResponse(
        json.dumps({
            "status": "ok",
            "message": "Hot-state cleared. All landing blobs will be re-processed on next refresh-hot call.",
            "file_deleted": existed
        }),
        status_code=200, mimetype="application/json"
    )


# ===========================================================================
# ROUTE 9 – DELETE /reset-all
# Nuclear reset for testing: wipes hot-state + all generated caches.
# Does NOT delete source landing JSON files or daily parquets.
# ===========================================================================

def reset_all() -> func.HttpResponse:
    """Wipe all caches and the hot-state. Use for a clean test run."""
    deleted = []
    skipped = []

    paths_to_delete = [
        HOT_STATE_BLOB,           # processed-blob tracker
        INDEX_BLOB_PATH,          # file index
        SUMMARY_BLOB_PATH,        # 10-day summary cache
        MONTHLY_SUMMARY_BLOB_PATH,  # monthly report cache
    ]

    # Also delete all per-user cache files
    user_cache_blobs = _list_blobs(USER_CACHE_PATH_PREFIX)
    paths_to_delete += user_cache_blobs

    for path in paths_to_delete:
        try:
            bc = _blob_client(path)
            if bc.exists():
                bc.delete_blob()
                deleted.append(path)
            else:
                skipped.append(path)
        except Exception as e:
            logging.warning(f"reset-all: could not delete '{path}': {e}")
            skipped.append(path)

    return func.HttpResponse(
        json.dumps({
            "status": "ok",
            "message": "All caches and state cleared. Run refresh-hot or refresh to rebuild.",
            "deleted": deleted,
            "not_found": skipped
        }),
        status_code=200, mimetype="application/json"
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
        all_users_param = req.params.get("all_users", "false").lower() == "true"

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
            # Build per-user JSON caches from the index (fast)
            return refresh_users(req)

        elif method == "POST" and action == "refresh-monthly":
            # Build monthly report cache from the index (fast)
            return refresh_monthly(req)

        elif method == "POST" and action == "refresh-hot":
            # ⚡ LIVE pipeline — call this from Logic App every 2 hours
            # Use ?force=true to reprocess an already-seen blob (testing)
            return refresh_hot(req)

        elif method == "POST" and action == "refresh":
            # Full rebuild of 10-day summary cache (daily cold run)
            return refresh_10day(req)

        elif method == "DELETE" and action == "reset-hot-state":
            # 🧪 Testing: clear the processed-blob tracker
            return reset_hot_state()

        elif method == "DELETE" and action == "reset-all":
            # 🧪 Testing: wipe all caches + state (does NOT delete source data)
            return reset_all()

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

        elif method == "GET" and all_users_param:
            # Instant: read monthly report cache
            logging.info(f"Reading monthly cache: {MONTHLY_SUMMARY_BLOB_PATH}")
            bc = _blob_client(MONTHLY_SUMMARY_BLOB_PATH)
            if not bc.exists():
                return func.HttpResponse(
                    json.dumps({"error": "Monthly report cache not found. Trigger POST /refresh-monthly first."}),
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
