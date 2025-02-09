import boto3
import json
import time
import yaml
import logger
from typing import Dict, Union

def get_config_params() -> Dict[str, Union[str, int]]:
    """
    Load configuration parameters from a YAML file.

    Returns:
        dict: A dictionary with SQS connection parameters:
            - region (str): The SQS region.
            - url (str): The SQS URL.
            - waittime (int): The port number.
            - maxmessages (int): The database name.
    """

    with open('config/config.yaml', 'r') as file: 
        config = yaml.safe_load(file)

    return {
        'region': config['Region'],
        'queue_url': config['SQS']['url'],
        'waittime': config['SQS']['url'],
        'maxmessages': config['SQS']['url']
    }

config_params = get_config_params()
sqs = boto3.client('sqs', region_name=config_params['Region'])  

def process_message(message):
    
    body = json.loads(message['Body']) # S3 messages comes in JSON format
    
    records = body.get('Records', [])
    for record in records:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        print(f"Nuevo archivo en S3: Bucket={bucket}, Key={key}")
        # TODO Aquí puedes invocar el proceso que lee el archivo, lo procesa, etc.

def poll_messages():
    """
    Por ahora en formato de loop infinito antes de debuguear
    """
    while True:
        # Long polling para obtener mensajes (espera hasta 20 segundos)
        response = sqs.receive_message(
            QueueUrl=config_params['queue_url'],
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20
        )

        messages = response.get('Messages', [])
        if not messages:
            print("No se encontraron mensajes, esperando...")
            continue
        
        print(messages)
        
        for message in messages:
            try:
                process_message(message)
                sqs.delete_message(
                    QueueUrl=config_params['queue_url'],
                    ReceiptHandle=message['ReceiptHandle']
                ) #Once proccesed the messega must be eliminated from the queue

            except Exception as error:
                logger.error("Error connecting to Redshift: %s", error)
                raise error
        time.sleep(1)

def delete_unhandeled_messages(receipt_handle):

    # Set the visibility timeout to 0 to make the message visible again.
    sqs.change_message_visibility(
        QueueUrl=config_params['queue_url'],
        ReceiptHandle=receipt_handle,
        VisibilityTimeout=0
    )

    sqs.delete_message(
        QueueUrl=config_params['queue_url'],
        ReceiptHandle=receipt_handle
    )