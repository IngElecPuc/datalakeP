import yaml
import boto3
from botocore.exceptions import ClientError

def get_secret(secret_name, region_name):
    """
    AWS base script for secret return
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

def get_config_params():
    """
    Load the configuration files in a single dictionary
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
