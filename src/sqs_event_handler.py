import boto3
import json
import yaml
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from typing import Dict, Union, Tuple, List

def get_sqs_config_params() -> Dict[str, Union[str, int]]:
    """
    Load configuration parameters from a YAML file.

    Returns:
        dict: A dictionary with SQS connection parameters:
            - region (str): The SQS region.
            - queue_url (str): The SQS URL.
            - waittime (int): The port number.
            - maxmessages (int): The database name.
    """

    with open('config/config.yaml', 'r') as file: 
        config = yaml.safe_load(file)

    return {
        'region': config['Region'],
        'queue_url': config['SQS']['url'],
        'waittime': config['SQS']['waittime'],
        'maxmessages': config['SQS']['maxmessages']
    }

config_params = get_sqs_config_params()
sqs = boto3.client('sqs', region_name=config_params['region'])  

def get_files_data(config_params: Dict[str, Union[str, int]]) -> Tuple[bool, List[Tuple[str, str]]]:
    """
    Retrieve S3 bucket and filename information from SQS messages if available.

    Parameters:
        config_params (dict[str, Union[str, int]]): A dictionary containing SQS queue parameters:
            - region (str): The SQS region.
            - queue_url (str): The SQS URL.
            - waittime (int): The wait time in seconds for long polling.
            - maxmessages (int): The maximum number of messages to retrieve.
    
    Returns:
        Tuple[bool, List[Tuple[str, str]]]:
            - bool: Flag indicating whether any messages were found.
            - list: A list of tuples, each containing (S3 bucket name, filename) for each received message.
    """
    
    new_files = []
    response = sqs.receive_message(
            QueueUrl=config_params['queue_url'],
            MaxNumberOfMessages=config_params['maxmessages'],
            WaitTimeSeconds=config_params['waittime']
        )
    messages = response.get('Messages', [])
    if not messages:
        return False, new_files #This is meant for a signal in the main loop -> no response, continue
    
    for message in messages:
        try:
            body = json.loads(message['Body']) # S3 messages comes in JSON format
            records = body.get('Records', [])
            for record in records:
                bucket = record['s3']['bucket']['name']
                key = record['s3']['object']['key']
                new_files.append((bucket, key))

            sqs.delete_message(
                QueueUrl=config_params['queue_url'],
                ReceiptHandle=message['ReceiptHandle']
            ) #Once proccesed the messega must be eliminated from the queue

        except Exception as error:
            logger.error("Error reading from SQS: %s", error)
            raise error
    return True, new_files

def delete_unhandled_messages(receipt_handle: str) -> None:
    """
    Erases rogue messages from the queue.

    Parameters:
        receipt_handle (str): The message's handler
    """

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