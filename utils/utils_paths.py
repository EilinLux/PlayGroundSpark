import os

# Path Operations
def join_paths(*args):
    return os.path.join(*args).replace("\\","/")

def list_dir(path):
    return os.listdir(path)

# GCP
def get_path_type(path):
        """ return the type of path.
            'gcs' if a google storage path
            'bq' if a bigquery path
            None if neither of the above ones.
             
        """
        type_ = 'gcs' if path.startswith('gs://') else 'bq' if ('/' not in path) and ('.' in path) else None
        return type_
