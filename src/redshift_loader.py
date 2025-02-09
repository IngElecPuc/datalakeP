import yaml
import boto3
import psycopg2
import logging
from sql_queries import QUERY_TABLE_NAMES, CREATE_GENDER_SUBMISSION, CREATE_TRAIN_TEST_DATA
from botocore.exceptions import ClientError
from typing import Dict, Union
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_secret(secret_name: str, region_name: str) -> str:
    """
    Retrieve a secret from AWS Secrets Manager.

    Parameters:
        secret_name (str): The name or ARN of the secret.
        region_name (str): The AWS region where the secret is stored.

    Returns:
        str: The secret value as a JSON string or plain string.

    Raises:
        ClientError: If the secret retrieval fails.
    """

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as error:
        logger.error("Error connecting to Secrets Manager: %s", error)
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise error

    secret = get_secret_value_response['SecretString']

    return secret

def get_config_params() -> Dict[str, Union[str, int]]:
    """
    Load configuration parameters from a YAML file and AWS Secrets Manager if necessary.

    Returns:
        dict: A dictionary with Redshift connection parameters:
            - host (str): The Redshift cluster endpoint.
            - port (int): The port number.
            - dbname (str): The database name.
            - user (str): The username.
            - password (str): The password retrieved from the config file or Secrets Manager.
    """

    with open('config/config.yaml', 'r') as file: 
        config = yaml.safe_load(file)

    # Redshift configuration parameters
    host = config['Redshift']['endpoint']
    port = config['Redshift']['port']
    dbname = config['Redshift']['db_name']
    user = config['Redshift']['user']
    if config['Redshift']['password'] != 'SecretsManager':
        password = config['Redshift']['password']
    else:
        password = get_secret(config['SecretsManager']['secret_name'], config['Region'])

    return {
        'host': host,
        'port': port,
        'dbname': dbname,
        'user': user,
        'password': password
    }

def ensure_required_tables(config_params: Dict[str, Union[str, int]]) -> None:
    """
    Connect to the Redshift database, check for existing tables, and create necessary tables if missing.

    Parameters:
        config_params (dict[str, Union[str, int]]): A dictionary containing Redshift connection parameters:
            - host: The Redshift cluster endpoint.
            - port: The port number.
            - dbname: The database name.
            - user: The username.
            - password: The password.

    Returns:
        None
    """

    try:
        # Establish connection to Redshift
        with psycopg2.connect(
            host=config_params['host'],
            port=config_params['port'],
            dbname=config_params['dbname'],
            user=config_params['user'],
            password=config_params['password']
        ) as connection:
            with connection.cursor() as cursor:

                table_queries = {
                    'gender_submission': CREATE_GENDER_SUBMISSION, 
                    'train_test_data': CREATE_TRAIN_TEST_DATA
                }
                
                cursor.execute(QUERY_TABLE_NAMES)
                tables = cursor.fetchall() #Obtain all tables and schemes
                redshift_tables  = []
                
                for _, tablename in tables:
                    redshift_tables.append(tablename)

                for table in table_queries.keys():
                    if table not in redshift_tables :
                        cursor.execute(table_queries[table])
                        connection.commit()
        
    except Exception as error:
        logger.error("Error connecting to Redshift: %s", error)
        raise error
        

def copy_data_from_s3_to_redshift(config_params: Dict[str, Union[str, int]], 
                                  s3_path: str, 
                                  target_table: str, 
                                  iam_role: str
                                  ) -> None:
    """
    Copy data from an S3 bucket to a Redshift table using the COPY command.

    Parameters:
        config_params (dict[str, Union[str, int]]): A dictionary containing Redshift connection parameters:
            - 'dbname': The database name.
            - 'user': The username.
            - 'password': The password.
            - 'host': The Redshift cluster endpoint.
            - 'port': The port number.
        s3_path (str): The full S3 path to the file, e.g., 's3://bucket_name/file.csv'.
        target_table (str): The name of the destination table in Redshift.
        iam_role (str): The ARN of the IAM role that grants access to S3.

    Returns:
        None

    Raises:
        Exception: If an error occurs during the data copy process.
    """

    copy_command = f"""
        COPY {target_table}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        CSV
        IGNOREHEADER 1;
    """
    
    try:
        with psycopg2.connect(
            host=config_params['host'],
            port=config_params['port'],
            dbname=config_params['dbname'],
            user=config_params['user'],
            password=config_params['password']
        ) as connection:
            with connection.cursor() as cursor:

                cursor.execute(copy_command)
                connection.commit()

    except Exception as error:
        logger.error("Error loading data to Redshift: %s", error)
        raise error
