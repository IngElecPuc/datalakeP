from redshift_loader import get_rs_config_params, ensure_required_tables, copy_data_from_s3_to_redshift, retrieve_table_names
from sqs_event_handler import get_sqs_config_params, get_files_data
from s3_preproc import load_file, check_columns, decide_table_for_file, format_for_table, save_dataframe_to_s3
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    
    redshift_config = get_rs_config_params()
    sqs_config = get_sqs_config_params()
    
    ensure_required_tables(redshift_config) #Check if the tables exist and initialize them if not

    #Loop to listen to messages
    while True:
        founded, files = get_files_data(sqs_config) #Served a message retrieve key and value from the S3 event
        if not founded:
            continue
        for bucket, key in files:
            targetfile, df = load_file(bucket, key) #Load a pandas DataFrame for preprocessing if necessary
            if targetfile:
                key = key.split('/')[1] #Expecting files from a special upload folder
                if check_columns(redshift_config, df, key): #Check if columns are compatible with known definitions
                    all_tables = retrieve_table_names(redshift_config)
                    table_name = decide_table_for_file(key, all_tables)
                    key2 = f"Tmp/{key.split('.')[0]}-autogen.csv" #A Tmp folder is needed in the bucket 
                    s3_path = f"s3://{bucket}/{key2}"
                    df = format_for_table(df, table_name, key) #Preproc
                    save_dataframe_to_s3(df, bucket, key2) #Save to csv format for COPY command
                    copy_data_from_s3_to_redshift(redshift_config, s3_path, table_name)
                    logger.info(f'File {key} from bucket {bucket} is a valid file')
            else:
                logger.info(f'File {key} detected, but not compatible')          


if __name__ == '__main__':
    main()