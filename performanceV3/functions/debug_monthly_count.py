from datetime import datetime
import pandas as pd
from performanceV3.common import MANUAL_STATUSES

def debug_count_files_by_month(df_all, username: str, month: int, year: int):
    """
    Debug function to count files created by a user in a specific month.
    Bypasses all 90-day logic - just raw counts from the data.
    
    Args:
        df_all: Full dataframe
        username: Username to search for
        month: Month number (1-12)
        year: Year (e.g., 2024)
    
    Returns:
        Dictionary with file counts and details
    """
    
    # Clean data
    df = df_all.copy()
    for col in ["USERCODE", "HISTORY_STATUS"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()
    
    df["HISTORYDATETIME"] = pd.to_datetime(df["HISTORYDATETIME"], errors="coerce", format="mixed")
    df = df.dropna(subset=["HISTORYDATETIME"])
    df["HISTORYDATETIME"] = df["HISTORYDATETIME"].dt.tz_localize(None)
    
    username = username.upper()
    
    # Filter to the specific month
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1)
    else:
        end_date = datetime(year, month + 1, 1)
    
    month_df = df[(df["HISTORYDATETIME"] >= start_date) & (df["HISTORYDATETIME"] < end_date)]
    
    # Get all files the user touched in this month
    user_actions = month_df[month_df["USERCODE"] == username]
    
    if user_actions.empty:
        return {
            "user": username,
            "month": f"{month:02d}/{year}",
            "total_actions": 0,
            "manual_files_created": 0,
            "files_created": [],
            "all_statuses": [],
            "message": "No actions found for this user in this month"
        }
    
    # Count files where user had manual creation status (NEW, COPY, COPIED)
    manual_creates = user_actions[user_actions["HISTORY_STATUS"].isin(MANUAL_STATUSES)]
    unique_manual_files = manual_creates["DECLARATIONID"].unique().tolist()
    
    # Also check for any files where the user's FIRST action globally was in this month
    all_user_files = df[df["USERCODE"] == username]
    grouped = all_user_files.groupby("DECLARATIONID")
    
    files_first_created_this_month = []
    for decl_id, group in grouped:
        first_action = group.sort_values("HISTORYDATETIME").iloc[0]
        if start_date <= first_action["HISTORYDATETIME"] < end_date:
            if first_action["HISTORY_STATUS"] in MANUAL_STATUSES:
                files_first_created_this_month.append({
                    "id": decl_id,
                    "first_action_date": first_action["HISTORYDATETIME"].isoformat(),
                    "status": first_action["HISTORY_STATUS"]
                })
    
    # Status breakdown for this month
    status_counts = user_actions["HISTORY_STATUS"].value_counts().to_dict()
    
    # Count total unique files touched
    total_files_touched = user_actions["DECLARATIONID"].nunique()
    
    return {
        "user": username,
        "month": f"{month:02d}/{year}",
        "period": f"{start_date.date()} to {end_date.date()}",
        "total_actions": len(user_actions),
        "total_files_touched": total_files_touched,
        "manual_creation_actions_in_month": len(manual_creates),
        "unique_files_with_manual_status": len(unique_manual_files),
        "files_first_created_in_month": len(files_first_created_this_month),
        "files_first_created_details": files_first_created_this_month[:20],  # First 20
        "status_breakdown": status_counts,
        "sample_actions": user_actions.head(10).to_dict(orient='records')
    }

def debug_total_files_for_user(df_all, username: str):
    """
    Count ALL files ever created by a user (no date filter).
    """
    df = df_all.copy()
    for col in ["USERCODE", "HISTORY_STATUS"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()
    
    username = username.upper()
    
    # Get all actions by this user
    user_actions = df[df["USERCODE"] == username]
    
    if user_actions.empty:
        return {
            "user": username,
            "total_actions": 0,
            "message": "No actions found for this user"
        }
    
    # Count files created with manual statuses
    manual_creates = user_actions[user_actions["HISTORY_STATUS"].isin(MANUAL_STATUSES)]
    unique_manual_files = manual_creates["DECLARATIONID"].unique()
    
    # Find files where user's FIRST action was manual
    grouped = user_actions.groupby("DECLARATIONID")
    files_created_by_user = []
    
    for decl_id, group in grouped:
        first_action = group.sort_values("HISTORYDATETIME").iloc[0]
        if first_action["HISTORY_STATUS"] in MANUAL_STATUSES:
            files_created_by_user.append(decl_id)
    
    return {
        "user": username,
        "total_actions": len(user_actions),
        "total_files_touched": user_actions["DECLARATIONID"].nunique(),
        "files_with_manual_status": len(unique_manual_files),
        "files_first_created_by_user": len(files_created_by_user),
        "status_breakdown": user_actions["HISTORY_STATUS"].value_counts().to_dict(),
        "unique_declaration_ids": sorted(user_actions["DECLARATIONID"].unique().tolist())[:50]  # First 50
    }

