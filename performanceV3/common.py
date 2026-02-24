from typing import List, Set, Tuple

# --- Constants ---

IMPORT_USERS = [
    'FADWA.ERRAZIKI', 'AYOUB.SOURISTE', 'AYMANE.BERRIOUA', 'SANA.IDRISSI', 'AMINA.SAISS',
    'KHADIJA.OUFKIR', 'ZOHRA.HMOUDOU', 'SIMO.ONSI', 'YOUSSEF.ASSABIR', 'ABOULHASSAN.AMINA',
    'MEHDI.OUAZIR', 'OUMAIMA.EL.OUTMANI', 'HAMZA.ALLALI', 'MUSTAPHA.BOUJALA', 'HIND.EZZAOUI',
    'MOHAMED.BOUIDAR', 'HOUDA.EZZAOUI', 'YAHYA.ANEJARN', 'BATCHPROC'
]

EXPORT_USERS = [
    'IKRAM.OULHIANE', 'MOURAD.ELBAHAZ', 'MOHSINE.SABIL', 'AYA.HANNI',
    'ZAHIRA.OUHADDA', 'CHAIMAAE.EJJARI', 'HAFIDA.BOOHADDOU', 'KHADIJA.HICHAMI', 'FATIMA.ZAHRA.BOUGSIM',
    'BATCHPROC'
]

# Combined list of all target users for reports
TARGET_USERS = list(dict.fromkeys(IMPORT_USERS + EXPORT_USERS)) # Unique list for individual processing

MANUAL_STATUSES = {"COPIED", "COPY", "NEW"}
SENDING_STATUSES = {"DEC_DAT"}
SYSTEM_USERS = {"BATCHPROC", "ADMIN", "SYSTEM", "BATCH_PROC"} # Unified system user identification

# --- Helper Functions ---

def classify_file_activity(global_history: List[str], user_history: List[str] = None, group_df=None, target_user: str = None) -> Tuple[bool, bool]:
    """
    Determines if a file activity is 'Automatic' or 'Manual'.
    
    Logic:
    - Automatic: If 'INTERFACE' is present in the GLOBAL history status.
    - Manual: If any of MANUAL_STATUSES are present in the USER history status (or global if not provided) AND it is NOT Automatic.
    - BATCHPROC handling: 
        - If file was created by BATCHPROC:
            - If ANY human (non-SYSTEM_USER) interacted with it (even if not in TARGET_USERS), the most active human gets credit.
            - If ONLY system users interacted, BATCHPROC gets credit.
    
    Args:
        global_history: Full history of the declaration (e.g. all users' actions).
        user_history: Specific history of the user being analyzed. If None, defaults to global_history.
        group_df: DataFrame of the entire file history (needed for BATCHPROC logic).
        target_user: The user being analyzed (needed for BATCHPROC logic).
    
    Returns:
        A tuple (is_manual, is_automatic)
    """
    
    # Ensure inputs are sets for faster lookup
    global_set = set(global_history)
    user_set = set(user_history) if user_history is not None else global_set
    
    is_automatic = 'INTERFACE' in global_set
    
    # Check if file was created by BATCHPROC
    if group_df is not None and target_user is not None:
        # Sort by time to get the first action
        sorted_group = group_df.sort_values('HISTORYDATETIME')
        if not sorted_group.empty:
            first_user = str(sorted_group.iloc[0]['USERCODE']).upper()
            if first_user in SYSTEM_USERS:
                is_automatic = True  # It's an automated file
                
                # Check if ANY human (non-system user) interacted with it
                # Logic: Is there a value in USERCODE not in SYSTEM_USERS?
                human_actions = group_df[~group_df['USERCODE'].astype(str).str.upper().isin(SYSTEM_USERS)]
                
                if not human_actions.empty:
                    # A human DID interact. BATCHPROC gets NO credit.
                    if target_user.upper() in SYSTEM_USERS:
                        return False, False
                    
                    # It's a human. Find the human with most MODIFIED actions.
                    # Note: We count ALL modifications to find the "responsible" human.
                    mod_counts = human_actions[human_actions['HISTORY_STATUS'] == 'MODIFIED']['USERCODE'].value_counts()
                    
                    if not mod_counts.empty:
                        responsible_human = mod_counts.idxmax()
                        if responsible_human.upper() == target_user.upper():
                            return False, True # Give this human the automatic file credit
                        else:
                            return False, False # Other humans get no credit for creation
                    else:
                        # No MODIFIED actions? Fallback to the human who did first action
                        first_human = human_actions.iloc[0]['USERCODE']
                        if first_human.upper() == target_user.upper():
                            return False, True
                        return False, False
                else:
                    # ONLY system users touched it.
                    if target_user.upper() == 'BATCHPROC':
                        return False, True
                    return False, False
    
    # Check intersection with manual statuses in the specific scope (User or Global)
    has_manual_trigger = bool(MANUAL_STATUSES.intersection(user_set))
    
    # Precise definition: Manual only if triggered manually AND not an interface file
    is_manual = has_manual_trigger and not is_automatic
    
    return is_manual, is_automatic
