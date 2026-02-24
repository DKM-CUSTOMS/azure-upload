# Get detailed breakdown for last 30 days
from performance.dms_functions import count_dms_import_files_created, get_dms_import_summary


result = count_dms_import_files_created(df, 30)
print(f"Total DMS_IMPORT files: {result['total_dms_import_files']}")
print(f"Users involved: {result['unique_users_count']}")

# Get quick summary for last 7 days
summary = get_dms_import_summary(df, 7)
print(f"Top user: {summary['top_5_users'][0]['user']} with {summary['top_5_users'][0]['files_created']} files")