from redshift_loader import get_rs_config_params, ensure_required_tables, copy_data_from_s3_to_redshift
from sqs_event_handler import get_sqs_config_params, get_files_data
from s3_preproc import check_file

def main():

    
    redshift_config = get_rs_config_params()
    sqs_config = get_sqs_config_params()
    
    ensure_required_tables(redshift_config) #Check if the tables exist and initialize them if not

    #Loop to listen to messages
    while True:

        founded, files = get_files_data(sqs_config) #Served a message retrieve key and value from the S3 event
        if not founded:
            continue
        extension = filename.split('.')[1]
        #Verificar si corresponde con un archivo excel
        if extension == 'xlsx' or extension == 'xls' or extension == 'csv':

            #Leer archivo y comprobar que corresponde con la naturaleza de alguna de las tablas -> prepocesamiento -> columnas
            check_file(s3_path, filename)
            #Cargar archivo
            copy_data_from_s3_to_redshift(redshift_config, s3_path, ..., filename) #Extender configuración de get_rs_config_params para sacar todo lo necesario

        pass

if __name__ == '__main__':
    main()