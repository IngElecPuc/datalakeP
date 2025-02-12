
import json
import boto3
import pandas as pd
from io import BytesIO, StringIO
import difflib
from typing import List, Tuple, Optional, Dict, Union
from redshift_loader import query_col_names, retrieve_table_names
import logging

logger = logging.getLogger(__name__)

s3 = boto3.client('s3')

def load_file(bucket: str, key: str) -> Tuple[bool, Optional[pd.DataFrame]]:
    """
    Load a file from an S3 bucket and convert it to a pandas DataFrame if the file extension is allowed.

    Parameters:
        bucket (str): The name of the S3 bucket.
        key (str): The key (path) of the file in the S3 bucket.

    Returns:
        Tuple[bool, Optional[pd.DataFrame]]:
            - bool: True if the file was successfully loaded and converted, False otherwise.
            - Optional[pd.DataFrame]: The resulting DataFrame if the file extension is allowed; otherwise, None.
    """
    allowed_extensions = {'xlsx', 'xls', 'csv'}
    extension = key.split('.')[-1].lower()  # Use the last part after a dot as extension

    if extension in allowed_extensions:
        # Replace '+' characters with spaces in the key.
        key = key.replace('+', ' ')
        response = s3.get_object(Bucket=bucket, Key=key)
        file_content = response['Body'].read()
        file_io = BytesIO(file_content)
        if extension == 'csv':
            df = pd.read_csv(file_io)
        else:
            df = pd.read_excel(file_io, engine='openpyxl')
        return True, df
    else:
        return False, None

def decide_table_for_file(filename: str, table_names: List[str], cutoff: float = 0.4) -> Optional[str]:
    """
    Decide which table to use based on the similarity between the filename and available table names.
    In the future we could augment precision by using RapidFuzz o fuzzywuzzy, or even a NLP best suited library.

    Parameters:
        filename (str): The name of the file (including extension) to be processed.
        table_names (List[str]): A list of available table names.
        cutoff (float): The minimum similarity ratio required to select a table (default is 0.4).

    Returns:
        Optional[str]: The table name that best matches the filename, or None if no match exceeds the cutoff.
    """
    # Remove the extension and convert to lowercase.
    base_name = filename.split('.')[0].lower()
    
    # Use difflib to obtain the closest match.
    matches = difflib.get_close_matches(base_name, table_names, n=1, cutoff=cutoff)
    
    if matches:
        return matches[0]
    else:
        return None


def check_columns(
    rs_config: Dict[str, Union[str, int]],
    df: pd.DataFrame,
    filename: str
    ) -> bool:
    """
    Check if the DataFrame columns match the expected columns for the Redshift table 
    inferred from the filename.

    This function retrieves the list of available table names from Redshift, determines 
    the appropriate table based on the filename, retrieves the expected column names for that table, 
    and then checks whether each column in the DataFrame (in lowercase) is present in the expected columns.

    Parameters:
        rs_config (dict[str, Union[str, int]]): A dictionary containing Redshift connection parameters:
            - host (str): The Redshift cluster endpoint.
            - port (int): The port number.
            - dbname (str): The database name.
            - user (str): The username.
            - password (str): The password.
        df (pd.DataFrame): The DataFrame containing the data to be loaded.
        filename (str): The filename of the file being processed.

    Returns:
        bool: True if all columns in the DataFrame are found in the expected column names; False otherwise.
    """
    all_tables = retrieve_table_names(rs_config)
    table_name = decide_table_for_file(filename, all_tables)
    
    if table_name is None:
        return False
        
    col_names = query_col_names(rs_config, table_name)
    check = True
    for column in df.columns:
        in_expected = column.lower() in col_names
        check = check and in_expected
    
    return check

def save_dataframe_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    """
    Saves a pandas DataFrame to an S3 bucket as a CSV file.

    Parameters:
      - df: DataFrame to be saved.
      - bucket: Name of the S3 bucket.
      - key: Path and file name within the bucket (e.g., 'folder/data.csv').
    """
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode('utf-8')
    s3.put_object(Bucket=bucket, Key=key, Body=csv_bytes)

def format_for_table(df: pd.DataFrame, table_name: str, filename: str) -> pd.DataFrame:
    """
    Format Dataframe to fit target's table format. 

    Parameters:
      - df: DataFrame to be formated.
      - table_name: Target table.
    """
    #pdb.set_trace()
    base_name = filename.split('.')[0].lower()
    candidates = ['train', 'test']
    matches = difflib.get_close_matches(base_name, candidates, n=1, cutoff=0.8)
    if not matches:
        matches = None
    else:
        matches = matches[0]
    
    if table_name == 'gender_submission':
        df['Survived'] = df['Survived'].map({0: 'false', 1: 'true'})
    elif table_name == 'train_test_data':
        df['ForTrain'] = matches == 'train'
    
    return df