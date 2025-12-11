class Path_Folder:
    processed_folder_path = "/opt/airflow/data/processed"
    raw_folder_path = "/opt/airflow/data/raw"


    def __init__(self):
        self.processed_folder_path = Path_Folder.processed_folder_path
        self.raw_folder_path = Path_Folder.raw_folder_path


# This class defines paths for processed and raw data folders.
# It can be used to access these paths in an Airflow DAG.   
    #   path_folder = Path_Folder()