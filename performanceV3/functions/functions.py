#-----------------------------------------------------------------------------
from collections import defaultdict, Counter
from datetime import datetime, timedelta
import pandas as pd
from performanceV3.common import IMPORT_USERS, EXPORT_USERS, TARGET_USERS, classify_file_activity, SENDING_STATUSES

def count_user_file_creations_last_10_days(df_all):
    # Use consolidated lists from common
    df = df_all.copy()
    users = TARGET_USERS

    # --- CHANGE 1: "INTERFACE" is no longer considered a manual status (Handled in common) ---

    # Clean columns like in calculate_single_user_metrics_fast
    for col in ["USERCREATE", "USERCODE", "HISTORY_STATUS", "ACTIVECOMPANY"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    # Filter out DKM_VP activity
    if 'ACTIVECOMPANY' in df.columns:
        df = df[df['ACTIVECOMPANY'] != 'DKM_VP']

    # Convert to datetime without 'mixed' format for broader compatibility
    df["HISTORYDATETIME"] = pd.to_datetime(df["HISTORYDATETIME"], errors="coerce")
    df = df.dropna(subset=["HISTORYDATETIME"])
    df["HISTORYDATETIME"] = df["HISTORYDATETIME"].dt.tz_localize(None)

    # Last 10 working days (Mon-Fri)
    today = datetime.now().date()
    working_days = []
    curr = today
    while len(working_days) < 10:
        if curr.weekday() < 5:
            working_days.insert(0, curr)
        curr -= timedelta(days=1)

    results = []

    # Process each team separately so BATCHPROC can appear in both
    for team_name, team_users in [("import", IMPORT_USERS), ("export", EXPORT_USERS)]:
        for user in team_users:
            user_daily = {day.strftime("%d/%m"): 0 for day in working_days}

            cutoff = datetime.now() - timedelta(days=90)
            recent_df = df[df["HISTORYDATETIME"] >= cutoff]
            
            # Special handling for BATCHPROC: filter by team
            if user == 'BATCHPROC':
                import_types = ['IDMS_IMPORT', 'DMS_IMPORT']
                if team_name == 'import':
                    recent_df = recent_df[recent_df['TYPEDECLARATIONSSW'].isin(import_types)]
                else:
                    recent_df = recent_df[~recent_df['TYPEDECLARATIONSSW'].isin(import_types)]

            user_decls = recent_df[recent_df["USERCODE"] == user]["DECLARATIONID"].unique()

            # Work on all declarations this user touched
            user_scope_df = df[df["DECLARATIONID"].isin(user_decls)].copy()
            
            # Deduplicate to prevent counting same action multiple times
            user_scope_df = user_scope_df.drop_duplicates(
                subset=['DECLARATIONID', 'USERCODE', 'HISTORY_STATUS', 'HISTORYDATETIME']
            )

            grouped = user_scope_df.groupby("DECLARATIONID")

            for decl_id, group in grouped:
                group = group.sort_values("HISTORYDATETIME")
                if group.empty:
                    continue

                user_rows = group[group["USERCODE"] == user]
                if user_rows.empty:
                    continue

                first_action_date = user_rows["HISTORYDATETIME"].min().date()
                if first_action_date not in working_days:
                    continue

                # --- CHANGE 2: Simplified automatic/manual classification logic ---
                is_manual, is_automatic = classify_file_activity(
                    global_history=group["HISTORY_STATUS"].tolist(),
                    user_history=user_rows["HISTORY_STATUS"].tolist(),
                    group_df=group,
                    target_user=user
                )
                # --- END OF CHANGES ---

                if is_manual or is_automatic:
                    key = first_action_date.strftime("%d/%m")
                    user_daily[key] += 1

            results.append({
                "user": user,
                "team": team_name,
                "daily_file_creations": user_daily
            })

    return results

def calculate_single_user_metrics_fast(df_all, username):
    df = df_all.copy()

    for col in ["USERCREATE", "USERCODE", "HISTORY_STATUS", "ACTIVECOMPANY"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    # Filter out DKM_VP activity
    if 'ACTIVECOMPANY' in df.columns:
        df = df[df['ACTIVECOMPANY'] != 'DKM_VP']

    df["HISTORYDATETIME"] = pd.to_datetime(df["HISTORYDATETIME"], errors="coerce")
    df = df.dropna(subset=["HISTORYDATETIME"])
    df["HISTORYDATETIME"] = df["HISTORYDATETIME"].dt.tz_localize(None)

    username = username.upper()
    
    # REMOVED 90-day cutoff - now includes ALL historical data to prevent data fade
    # Get ALL declarations this user has ever touched (no time limit)
    
    # Special handling for BATCHPROC if we need to split it here too
    # However, calculate_single_user_metrics_fast is usually called for ONE specific user view.
    # To determine team correctly for BATCHPROC, we check if they have more import or export files generally,
    # or we can allow the caller to specify. For now, let's keep it consistent.
    
    user_decls = df[df["USERCODE"] == username]["DECLARATIONID"].unique()
    if len(user_decls) == 0:
        return {
            "user": username,
            "daily_metrics": [],
            "summary": {}
        }

    user_scope_df = df[df["DECLARATIONID"].isin(user_decls)].copy()
    
    # Deduplicate to prevent counting same action multiple times
    user_scope_df = user_scope_df.drop_duplicates(
        subset=['DECLARATIONID', 'USERCODE', 'HISTORY_STATUS', 'HISTORYDATETIME']
    )
    
    # --- CHANGE 1: "INTERFACE" is no longer considered a manual status ---

    daily_summary = defaultdict(lambda: {
        "manual_files_created": 0,
        "automatic_files_created": 0,
        "sending_count": 0,
        "modification_count": 0,
        "modification_file_ids": set(),
        "total_files_handled": set(),
        "file_creation_times": [],
        "manual_file_ids": [],
        "automatic_file_ids": [],
        "sending_file_ids": []
    })

    grouped = user_scope_df.groupby("DECLARATIONID")

    for decl_id, group in grouped:
        group = group.sort_values("HISTORYDATETIME")
        if group.empty:
            continue
        
        user_rows = group[group["USERCODE"] == username]
        if user_rows.empty:
            continue

        first_action_date = user_rows["HISTORYDATETIME"].min().date().isoformat()
        
        # --- CHANGE 2: Simplified automatic/manual classification logic ---
        is_manual, is_automatic = classify_file_activity(
            global_history=group["HISTORY_STATUS"].tolist(),
            user_history=user_rows["HISTORY_STATUS"].tolist(),
            group_df=group,
            target_user=username
        )
        # --- END OF CHANGES ---

        # Get user's actions for this file (needed for both classification and lifecycle)
        mods = group[group["USERCODE"] == username]

        if is_manual:
            daily_summary[first_action_date]["manual_files_created"] += 1
            daily_summary[first_action_date]["total_files_handled"].add(decl_id)
            daily_summary[first_action_date]["manual_file_ids"].append(decl_id)
        elif is_automatic:
            # Credit for the automatic file is given if the user has interacted with it.
            daily_summary[first_action_date]["automatic_files_created"] += 1
            daily_summary[first_action_date]["total_files_handled"].add(decl_id)
            daily_summary[first_action_date]["automatic_file_ids"].append(decl_id)
        
        # Check for DEC_DAT (Sending) and MODIFIED (Modifications)
        # These can happen on any file, even if the user didn't create/auto-trigger it
        user_sending_rows = mods[mods["HISTORY_STATUS"].isin(SENDING_STATUSES)]
        if not user_sending_rows.empty:
            daily_summary[first_action_date]["sending_count"] += len(user_sending_rows)
            daily_summary[first_action_date]["total_files_handled"].add(decl_id)
            daily_summary[first_action_date]["sending_file_ids"].append(decl_id)

        # Count modifications (excluding the creation action if it was manual)
        # Actually, let's just count all MODIFIED statuses
        mod_rows = mods[mods["HISTORY_STATUS"] == "MODIFIED"]
        if not mod_rows.empty:
            daily_summary[first_action_date]["modification_count"] += len(mod_rows)
            daily_summary[first_action_date]["total_files_handled"].add(decl_id)
            daily_summary[first_action_date]["modification_file_ids"].add(decl_id)

        # File lifecycle logic (calculate time between MODIFIED and WRT_ENT)
        session_start = None
        for _, row in mods.sort_values("HISTORYDATETIME").iterrows():
            if row["HISTORY_STATUS"] == "MODIFIED" and session_start is None:
                session_start = row["HISTORYDATETIME"]
            elif row["HISTORY_STATUS"] == "WRT_ENT" and session_start:
                duration = (row["HISTORYDATETIME"] - session_start).total_seconds() / 3600
                daily_summary[first_action_date]["file_creation_times"].append(duration)
                session_start = None

    # Build daily metrics
    daily_metrics = []
    for date in sorted(daily_summary.keys()):
        data = daily_summary[date]
        avg_creation_time = (sum(data["file_creation_times"]) / len(data["file_creation_times"])) if data["file_creation_times"] else None
        daily_metrics.append({
            "date": date,
            "manual_files_created": data["manual_files_created"],
            "automatic_files_created": data["automatic_files_created"],
            "sending_count": data["sending_count"],
            "modification_count": data["modification_count"],
            "modification_file_ids": list(data["modification_file_ids"]),
            "total_files_handled": len(data["total_files_handled"]),
            "avg_creation_time": round(avg_creation_time, 2) if avg_creation_time else None,
            "manual_file_ids": data["manual_file_ids"],
            "automatic_file_ids": data["automatic_file_ids"],
            "sending_file_ids": list(set(data["sending_file_ids"]))
        })

    total_manual = sum(d["manual_files_created"] for d in daily_metrics)
    total_auto = sum(d["automatic_files_created"] for d in daily_metrics)
    total_sending = sum(d["sending_count"] for d in daily_metrics)
    total_mods = sum(d["modification_count"] for d in daily_metrics)
    total_handled = sum(d["total_files_handled"] for d in daily_metrics)

    all_creation_times = [t for d in daily_summary.values() for t in d["file_creation_times"]]
    avg_creation_time_total = (sum(all_creation_times) / len(all_creation_times)) if all_creation_times else None

    df_user_summary = user_scope_df[user_scope_df["USERCODE"] == username]
    file_type_counts = df_user_summary["TYPEDECLARATIONSSW"].value_counts().to_dict()
    activity_by_hour = df_user_summary["HISTORYDATETIME"].dt.hour.value_counts().sort_index().to_dict()
    company_specialization = df_user_summary["ACTIVECOMPANY"].value_counts().to_dict()

    most_productive_day = max(daily_metrics, key=lambda d: d["total_files_handled"], default={"date": None})["date"] if daily_metrics else None

    # Smart avg per day (only weekdays + at least 1 file created)
    valid_days = [
        d for d in daily_metrics
        if (datetime.strptime(d["date"], "%Y-%m-%d").weekday() < 5) and
        ((d["manual_files_created"] + d["automatic_files_created"]) > 0)
    ]
    total_created = sum(d["manual_files_created"] + d["automatic_files_created"] for d in valid_days)
    avg_files_per_day = round(total_created / len(valid_days), 2) if valid_days else 0

    days_active = len(valid_days)
    modification_file_ids = set()
    for d in daily_metrics:
        modification_file_ids.update(d["modification_file_ids"])
    modifications_per_file = round(total_mods / len(modification_file_ids), 2) if modification_file_ids else 0

    manual_vs_auto_ratio = {
        "manual_percent": round((total_manual / total_handled) * 100, 2) if total_handled else 0,
        "automatic_percent": round((total_auto / total_handled) * 100, 2) if total_handled else 0,
    }

    activity_days = df_user_summary["HISTORYDATETIME"].dt.date.value_counts().to_dict()
    all_days = set((datetime.now() - timedelta(days=i)).date() for i in range(90))
    inactive_days = sorted([d.isoformat() for d in all_days if d not in activity_days])

    hour_with_most_activity = max(activity_by_hour.items(), key=lambda x: x[1], default=(None, None))[0]

    return {
        "user": username,
        "daily_metrics": daily_metrics,
        "summary": {
            "total_manual_files": total_manual,
            "total_automatic_files": total_auto,
            "total_sent_files": total_sending,
            "total_files_handled": total_handled,
            "total_modifications": total_mods,
            "avg_files_per_day": avg_files_per_day,
            "avg_creation_time": round(avg_creation_time_total, 2) if avg_creation_time_total else None,
            "most_productive_day": most_productive_day,
            "file_type_counts": file_type_counts,
            "activity_by_hour": activity_by_hour,
            "company_specialization": company_specialization,
            "days_active": days_active,
            "modifications_per_file": modifications_per_file,
            "manual_vs_auto_ratio": manual_vs_auto_ratio,
            "activity_days": {str(k): int(v) for k, v in activity_days.items()},
            "inactivity_days": inactive_days,
            "hour_with_most_activity": hour_with_most_activity
        }
    }

def calculate_all_users_monthly_metrics(df_all):
    """
    Calculate file creation metrics for a specific list of users in the last month (30 days)
    Returns summary of files created and daily averages per user
    """
    df = df_all.copy()
    
    # --- NEW: Use consolidated list of users from common ---
    target_users = TARGET_USERS

    
    # Data preprocessing
    for col in ["USERCREATE", "USERCODE", "HISTORY_STATUS", "ACTIVECOMPANY"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()
    
    # Filter out DKM_VP activity
    if 'ACTIVECOMPANY' in df.columns:
        df = df[df['ACTIVECOMPANY'] != 'DKM_VP']
    
    # Convert to datetime without 'mixed' format for broader compatibility
    df["HISTORYDATETIME"] = pd.to_datetime(df["HISTORYDATETIME"], errors="coerce")
    df = df.dropna(subset=["HISTORYDATETIME"])
    df["HISTORYDATETIME"] = df["HISTORYDATETIME"].dt.tz_localize(None)
    
    # Filter for last 30 days
    cutoff = datetime.now() - timedelta(days=30)
    recent_df = df[df["HISTORYDATETIME"] >= cutoff]
    
    if recent_df.empty:
        return []
    
    if recent_df.empty:
        return []
    
    user_results = []
    
    # Process each team separately so BATCHPROC can appear in both
    for team_name, team_users in [("import", IMPORT_USERS), ("export", EXPORT_USERS)]:
        for username in team_users:
            # Get declarations this user worked on
            current_recent_df = recent_df.copy()
            
            # Special handling for BATCHPROC: filter by team
            if username == 'BATCHPROC':
                import_types = ['IDMS_IMPORT', 'DMS_IMPORT']
                if team_name == 'import':
                    current_recent_df = current_recent_df[current_recent_df['TYPEDECLARATIONSSW'].isin(import_types)]
                else:
                    current_recent_df = current_recent_df[~current_recent_df['TYPEDECLARATIONSSW'].isin(import_types)]

            user_decls = current_recent_df[current_recent_df["USERCODE"] == username]["DECLARATIONID"].unique()
            if len(user_decls) == 0:
                continue
            
            # Get all activity for these declarations
            user_scope_df = df[df["DECLARATIONID"].isin(user_decls)].copy()
            
            # Deduplicate to prevent counting same action multiple times
            user_scope_df = user_scope_df.drop_duplicates(
                subset=['DECLARATIONID', 'USERCODE', 'HISTORY_STATUS', 'HISTORYDATETIME']
            )
            
            # Track daily file creation
            daily_files = defaultdict(lambda: {
                "manual_files": 0,
                "automatic_files": 0,
                "sent_files": 0,
                "total_files": set()
            })
            
            # Group by declaration to analyze file creation logic
            grouped = user_scope_df.groupby("DECLARATIONID")
            
            for decl_id, group in grouped:
                group = group.sort_values("HISTORYDATETIME")
                if group.empty:
                    continue
                
                user_rows = group[group["USERCODE"] == username]
                if user_rows.empty:
                    continue
                
                user_actions_in_period = user_rows[user_rows["HISTORYDATETIME"] >= cutoff]
                if user_actions_in_period.empty:
                    continue
                    
                first_action_date = user_actions_in_period["HISTORYDATETIME"].min().date().isoformat()
                
                # Check for Sending (DEC_DAT)
                sent_count = len(user_actions_in_period[user_actions_in_period["HISTORY_STATUS"].isin(SENDING_STATUSES)])
                if sent_count > 0:
                    daily_files[first_action_date]["sent_files"] += sent_count
                    daily_files[first_action_date]["total_files"].add(decl_id)

                is_manual, is_automatic = classify_file_activity(
                    global_history=group["HISTORY_STATUS"].tolist(),
                    user_history=user_rows["HISTORY_STATUS"].tolist(),
                    group_df=group,
                    target_user=username
                )
                
                if is_manual:
                    daily_files[first_action_date]["manual_files"] += 1
                    daily_files[first_action_date]["total_files"].add(decl_id)
                elif is_automatic:
                    daily_files[first_action_date]["automatic_files"] += 1
                    daily_files[first_action_date]["total_files"].add(decl_id)
            
            # Calculate totals and averages
            total_manual = sum(day_data["manual_files"] for day_data in daily_files.values())
            total_automatic = sum(day_data["automatic_files"] for day_data in daily_files.values())
            total_sent = sum(day_data["sent_files"] for day_data in daily_files.values())
            
            # total_files_handled should be based on unique creations (manual + automatic)
            total_creations = total_manual + total_automatic
            
            # Identify valid days (weekdays where at least ONE creation or sending happened)
            # But the average calculation below will only use creations as per your request.
            valid_days_creations = []
            valid_days_any = 0
            for date_str, day_data in daily_files.items():
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                creations_today = day_data["manual_files"] + day_data["automatic_files"]
                activity_today = creations_today + day_data["sent_files"]
                
                if date_obj.weekday() < 5:
                    if activity_today > 0:
                        valid_days_any += 1
                    if creations_today > 0:
                        valid_days_creations.append(creations_today)
            
            # avg_activity_per_day now uses creations (384 / 22 style)
            avg_creations_per_day = round(sum(valid_days_creations) / len(valid_days_creations), 2) if valid_days_creations else 0
            
            user_results.append({
                "user": username,
                "team": team_name,
                "total_files_handled": total_creations, # Reflects unique files created
                "manual_files": total_manual,
                "automatic_files": total_automatic,
                "sent_files": total_sent,
                "days_with_activity": valid_days_any,
                "avg_activity_per_day": avg_creations_per_day, # Now based on creations
                "manual_vs_auto_ratio": {
                    "manual_percent": round((total_manual / total_creations) * 100, 2) if total_creations else 0,
                    "automatic_percent": round((total_automatic / total_creations) * 100, 2) if total_creations else 0
                }
            })
    
    return user_results
