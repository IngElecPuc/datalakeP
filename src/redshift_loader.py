import yaml
import boto3
import psycopg2
from botocore.exceptions import ClientError
from typing import Dict, Union

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
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

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
        connection = psycopg2.connect(
            host=config_params['host'],
            port=config_params['port'],
            dbname=config_params['dbname'],
            user=config_params['user'],
            password=config_params['password']
        )
        cursor = connection.cursor()
        
        # Query available tables, excluding internal schemes
        query_table_names = """
        SELECT schemaname, tablename
        FROM pg_catalog.pg_tables
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema');
        """

        create_gender_submission = """
        CREATE TABLE gender_submission(
        id INTEGER IDENTITY(1,1),
        PassengerId INTEGER NOT NULL,
        Survived BOOLEAN NOT NULL
        ) DISTSTYLE AUTO;
        """

        create_train_test_data = """
        CREATE TABLE train_test_data(
        id INTEGER IDENTITY(1,1),
        PassengerId INTEGER NOT NULL,
        Survived BOOLEAN NOT NULL,
        Pclass INTEGER NOT NULL,
        [Name] VARCHAR(100) NOT NULL,
        Sex	VARCHAR(6) NOT NULL,
        [Age] Decimal(4,2) NULL,
        SibSp INTEGER NOT NULL,	
        Parch INTEGER NOT NULL,
        Ticket VARCHAR(20) NOT NULL,
        Fare REAL NOT NULL,
        Cabin VARCHAR(15) NULL,
        Embarked CHAR(1) NULL,
        ForTrain BOOLEAN NOT NULL
        ) DISTSTYLE AUTO;
        """

        table_querys = {
            'gender_submission': create_gender_submission, 
            'train_test_data': create_train_test_data
        }
        
        cursor.execute(query_table_names)
        tables = cursor.fetchall() #Obtain all tables and schemes
        redshift_tables  = []
        
        for _, tablename in tables:
            redshift_tables.append(tablename)

        for table in table_querys.keys():
            if table not in redshift_tables :
                cursor.execute(table_querys[table])
                connection.commit()
        
    except Exception as error:
        print("Error al conectar o consultar en Redshift:", error)
        
    finally:
        if 'cursor' in locals(): #Close cursor
            cursor.close()
        if 'connection' in locals(): #Close connection
            connection.close()