import auth
import anaplan_functions

USERNAME = ""
PASSWORD = ""

SOURCE_MODEL_ID  = ""
SOURCE_WORKSPACE_ID = ""
SOURCE_EXPORT_NAME = ""

TARGET_MODEL_ID = ""
TARGET_WORKSPACE_ID = ""
TARGET_IMPORT_NAME = ""
TARGET_FILE_NAME = ""

def main():
    token = auth.get_token(USERNAME, PASSWORD)
    source_data = anaplan_functions.run_export_with_name(token, SOURCE_WORKSPACE_ID, SOURCE_MODEL_ID, SOURCE_EXPORT_NAME)
    anaplan_functions.import_df_with_names(token, TARGET_WORKSPACE_ID, TARGET_MODEL_ID, TARGET_IMPORT_NAME, source_data)

main()