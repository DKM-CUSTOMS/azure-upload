from collections import defaultdict, Counter
from datetime import datetime, timedelta
import pandas as pd
from performanceV3.common import classify_file_activity

def count_dms_import_files_created(df_all, days_back=30):
    """
    Count all files created with file type 'DMS_IMPORT' and track which users created them.
    
    Parameters:
    - df_all: DataFrame containing all the data
    - days_back: Number of days to look back (default: 30)
    
    Returns:
    - Dictionary with total count, user breakdown, and daily breakdown
    """
    df = df_all.copy()
    
    # Clean and preprocess data
    for col in ["USERCREATE", "USERCODE", "HISTORY_STATUS", "TYPEDECLARATIONSSW"]:
        df[col] = df[col].astype(str).str.strip().str.upper()
    
    df["HISTORYDATETIME"] = pd.to_datetime(df["HISTORYDATETIME"], errors="coerce", format="mixed")
    df = df.dropna(subset=["HISTORYDATETIME"])
    df["HISTORYDATETIME"] = df["HISTORYDATETIME"].dt.tz_localize(None)
    
    # Filter for the specified time period
    cutoff = datetime.now() - timedelta(days=days_back)
    recent_df = df[df["HISTORYDATETIME"] >= cutoff]
    
    # Filter for DMS_IMPORT file type only
    dms_import_df = recent_df[recent_df["TYPEDECLARATIONSSW"] == "DMS_IMPORT"]
    
    if dms_import_df.empty:
        return {
            "total_dms_import_files": 0,
            "users_breakdown": {},
            "daily_breakdown": {},
            "period_days": days_back,
            "file_details": []
        }
    
    
    # Track file creation by user and date
    user_files_count = defaultdict(lambda: {
        "manual_files": 0,
        "automatic_files": 0,
        "total_files": 0,
        "file_ids": []
    })
    
    daily_breakdown = defaultdict(lambda: {
        "manual_files": 0,
        "automatic_files": 0,
        "total_files": 0,
        "users": set(),
        "file_ids": []
    })
    
    file_details = []
    
    # Group by declaration to analyze file creation
    grouped = dms_import_df.groupby("DECLARATIONID")
    
    for decl_id, group in grouped:
        group = group.sort_values("HISTORYDATETIME")
        if group.empty:
            continue
        
        # Find the first user action on this file
        first_action = group.iloc[0]
        creation_date = first_action["HISTORYDATETIME"].date().isoformat()
        creator_user = first_action["USERCODE"]
        
        # --- REFACTORED: Use shared classification logic ---
        is_manual, is_automatic = classify_file_activity(group["HISTORY_STATUS"].tolist())
        # ---------------------------------------------------
        
        # Count the file for the creator
        if is_manual:
            user_files_count[creator_user]["manual_files"] += 1
            daily_breakdown[creation_date]["manual_files"] += 1
        elif is_automatic:
            user_files_count[creator_user]["automatic_files"] += 1
            daily_breakdown[creation_date]["automatic_files"] += 1
        
        user_files_count[creator_user]["total_files"] += 1
        user_files_count[creator_user]["file_ids"].append(decl_id)
        
        daily_breakdown[creation_date]["total_files"] += 1
        daily_breakdown[creation_date]["users"].add(creator_user)
        daily_breakdown[creation_date]["file_ids"].append(decl_id)
        
        # Store file details
        file_details.append({
            "declaration_id": decl_id,
            "creator": creator_user,
            "creation_date": creation_date,
            "creation_datetime": first_action["HISTORYDATETIME"].isoformat(),
            "file_type": "manual" if is_manual else "automatic",
            "company": first_action.get("ACTIVECOMPANY", "N/A")
        })
    
    # Convert defaultdicts to regular dicts and clean up
    users_breakdown = {}
    for user, data in user_files_count.items():
        users_breakdown[user] = {
            "total_files": data["total_files"],
            "manual_files": data["manual_files"],
            "automatic_files": data["automatic_files"],
            "manual_percentage": round((data["manual_files"] / data["total_files"]) * 100, 2) if data["total_files"] > 0 else 0,
            "file_ids": data["file_ids"]
        }
    
    daily_breakdown_clean = {}
    for date, data in daily_breakdown.items():
        daily_breakdown_clean[date] = {
            "total_files": data["total_files"],
            "manual_files": data["manual_files"],
            "automatic_files": data["automatic_files"],
            "unique_users": len(data["users"]),
            "users": list(data["users"]),
            "file_ids": data["file_ids"]
        }
    
    total_files = sum(data["total_files"] for data in users_breakdown.values())
    total_manual = sum(data["manual_files"] for data in users_breakdown.values())
    total_automatic = sum(data["automatic_files"] for data in users_breakdown.values())
    
    return {
        "total_dms_import_files": total_files,
        "total_manual_files": total_manual,
        "total_automatic_files": total_automatic,
        "manual_vs_auto_ratio": {
            "manual_percent": round((total_manual / total_files) * 100, 2) if total_files > 0 else 0,
            "automatic_percent": round((total_automatic / total_files) * 100, 2) if total_files > 0 else 0
        },
        "users_breakdown": users_breakdown,
        "daily_breakdown": daily_breakdown_clean,
        "period_days": days_back,
        "unique_users_count": len(users_breakdown),
        "file_details": sorted(file_details, key=lambda x: x["creation_datetime"], reverse=True)
    }

def get_dms_import_summary(df_all, days_back=30):
    """
    Get a quick summary of DMS_IMPORT file creation statistics.
    
    Parameters:
    - df_all: DataFrame containing all the data
    - days_back: Number of days to look back (default: 30)
    
    Returns:
    - Simplified summary dictionary
    """
    result = count_dms_import_files_created(df_all, days_back)
    
    # Get top users by file count
    top_users = sorted(
        result["users_breakdown"].items(), 
        key=lambda x: x[1]["total_files"], 
        reverse=True
    )[:5]
    
    return {
        "period": f"Last {days_back} days",
        "total_dms_import_files": result["total_dms_import_files"],
        "total_users_involved": result["unique_users_count"],
        "manual_vs_automatic": result["manual_vs_auto_ratio"],
        "top_5_users": [
            {
                "user": user,
                "files_created": data["total_files"],
                "manual_files": data["manual_files"],
                "automatic_files": data["automatic_files"]
            }
            for user, data in top_users
        ],
        "daily_average": round(result["total_dms_import_files"] / days_back, 2) if days_back > 0 else 0
    }