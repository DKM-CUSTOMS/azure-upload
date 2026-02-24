from datetime import datetime
import pandas as pd

def get_file_lifecycle(df_all, declaration_id: str):
    """
    Get complete lifecycle data for a specific declaration ID.
    
    Args:
        df_all: Full dataframe
        declaration_id: The declaration ID to search for
    
    Returns:
        Dictionary with lifecycle information
    """
    import logging
    
    # Convert DECLARATIONID to string for consistent comparison
    df_all = df_all.copy()
    
    logging.info(f"🔍 Searching for declaration_id: '{declaration_id}'")
    logging.info(f"📊 DataFrame shape: {df_all.shape}")
    logging.info(f"📋 DataFrame columns: {list(df_all.columns)}")
    
    # Check if DECLARATIONID column exists
    if 'DECLARATIONID' not in df_all.columns:
        logging.error("❌ DECLARATIONID column not found in DataFrame!")
        return {
            "found": False,
            "declaration_id": declaration_id,
            "message": "DECLARATIONID column not found in data",
            "available_columns": list(df_all.columns)
        }
    
    # Show sample of IDs
    sample_ids = df_all["DECLARATIONID"].head(10).tolist()
    logging.info(f"📝 Sample IDs from data: {sample_ids}")
    logging.info(f"🔢 ID data type in DataFrame: {df_all['DECLARATIONID'].dtype}")
    
    # Convert to int first (to remove .0), then to string
    df_all["DECLARATIONID"] = df_all["DECLARATIONID"].fillna(0).astype(int).astype(str)
    
    # Also ensure search term is string (and remove any .0 if present)
    try:
        declaration_id = str(int(float(declaration_id))).strip()
    except (ValueError, TypeError):
        declaration_id = str(declaration_id).strip()
    
    logging.info(f"🔎 After conversion - searching for: '{declaration_id}'")
    logging.info(f"📊 Total unique IDs in data: {df_all['DECLARATIONID'].nunique()}")
    
    # Filter to only this declaration
    file_data = df_all[df_all["DECLARATIONID"] == declaration_id].copy()
    
    logging.info(f"✅ Found {len(file_data)} rows for ID '{declaration_id}'")
    
    if file_data.empty:
        # Check if similar IDs exist
        similar = df_all[df_all["DECLARATIONID"].str.contains(declaration_id[:3], na=False)].head(5)
        similar_ids = similar["DECLARATIONID"].tolist() if not similar.empty else []
        
        return {
            "found": False,
            "declaration_id": declaration_id,
            "message": "No data found for this declaration ID",
            "similar_ids": similar_ids,
            "total_ids_in_system": int(df_all["DECLARATIONID"].nunique())
        }
    
    # Sort by time to get chronological order
    file_data = file_data.sort_values("HISTORYDATETIME")
    
    # Build timeline
    timeline = []
    for _, row in file_data.iterrows():
        # Handle datetime conversion safely
        timestamp = row["HISTORYDATETIME"]
        if pd.notna(timestamp):
            if isinstance(timestamp, str):
                timestamp_str = timestamp
            else:
                timestamp_str = timestamp.isoformat()
        else:
            timestamp_str = None
            
        timeline.append({
            "timestamp": timestamp_str,
            "user": row["USERCODE"],
            "status": row["HISTORY_STATUS"],
            "company": row.get("ACTIVECOMPANY", "N/A"),
            "type": row.get("TYPEDECLARATIONSSW", "N/A")
        })
    
    # Calculate metrics
    first_action = file_data.iloc[0]
    last_action = file_data.iloc[-1]
    
    # Get unique users involved
    users_involved = file_data["USERCODE"].unique().tolist()
    
    # Calculate duration
    first_dt = first_action["HISTORYDATETIME"]
    last_dt = last_action["HISTORYDATETIME"]
    
    if pd.notna(first_dt) and pd.notna(last_dt):
        # Ensure they are datetime objects
        if isinstance(first_dt, str):
            first_dt = pd.to_datetime(first_dt)
        if isinstance(last_dt, str):
            last_dt = pd.to_datetime(last_dt)
        duration_hours = (last_dt - first_dt).total_seconds() / 3600
    else:
        duration_hours = None
    
    # Count actions per user
    user_action_counts = file_data["USERCODE"].value_counts().to_dict()
    
    # Detect file type (manual vs automatic)
    has_interface = 'INTERFACE' in file_data["HISTORY_STATUS"].values
    has_manual_trigger = bool({'NEW', 'COPY', 'COPIED'}.intersection(set(file_data["HISTORY_STATUS"].values)))
    
    if has_interface:
        file_type = "Automatic (Interface)"
    elif has_manual_trigger:
        file_type = "Manual"
    else:
        file_type = "Unknown"
    
    # Identify creator
    creator = first_action["USERCODE"]
    if creator == "BATCHPROC":
        # Find user with most modifications
        human_mods = file_data[
            (file_data["USERCODE"] != "BATCHPROC") & 
            (file_data["HISTORY_STATUS"] == "MODIFIED")
        ]
        if not human_mods.empty:
            responsible_user = human_mods["USERCODE"].value_counts().idxmax()
        else:
            responsible_user = "BATCHPROC (No human interaction)"
    else:
        responsible_user = creator
    
    # Status breakdown
    status_counts = file_data["HISTORY_STATUS"].value_counts().to_dict()
    
    return {
        "found": True,
        "declaration_id": declaration_id,
        "file_type": file_type,
        "creator": creator,
        "responsible_user": responsible_user,
        "users_involved": users_involved,
        "user_action_counts": user_action_counts,
        "total_actions": len(file_data),
        "status_breakdown": status_counts,
        "first_action": {
            "timestamp": first_action["HISTORYDATETIME"].isoformat() if pd.notna(first_action["HISTORYDATETIME"]) and hasattr(first_action["HISTORYDATETIME"], 'isoformat') else str(first_action["HISTORYDATETIME"]) if pd.notna(first_action["HISTORYDATETIME"]) else None,
            "user": first_action["USERCODE"],
            "status": first_action["HISTORY_STATUS"]
        },
        "last_action": {
            "timestamp": last_action["HISTORYDATETIME"].isoformat() if pd.notna(last_action["HISTORYDATETIME"]) and hasattr(last_action["HISTORYDATETIME"], 'isoformat') else str(last_action["HISTORYDATETIME"]) if pd.notna(last_action["HISTORYDATETIME"]) else None,
            "user": last_action["USERCODE"],
            "status": last_action["HISTORY_STATUS"]
        },
        "duration_hours": round(duration_hours, 2) if duration_hours else None,
        "timeline": timeline
    }
