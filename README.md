# datalakeP
Proyecto de manejo de datos de práctica

Este es un proyecto de prueba que carga datos de un archivo Excel con servicios AWS, desde un bucket S3 a un clúster Redshift. Cuando un archivo es cargado en el bucket, S3 envía un mensaje a una cola SQS que la aplicación consume. 

![Diagrama_datalakep.png](/resources/Diagrama_datalakep.png "Diagrama de flujo datalakep.")

La aplicación está diseñada para correr en un contenedor docker, por lo que está pensada para dos modos diferentes de despliegue. Primero, la aplicación puede ser lanzada en una función Lambda que soporte el contenedor. De esta manera solo se pagan costes por las llamadas. La lambda recibe el evento de creación de objeto directo desde S3, e inicia un proceso ETL simple con la que solo preprocesa el archivo para verificar compatibilidad de lo recibido con el evento con las tablas de Redshift. Finalmente se produce la ingesta al clúster Redshift. 

El segundo modo de despliegue está pensado para archivos más pesados. Se debe mantener corriendo una instancia EC2 que será notificada a través de una cola de mensajes SQS. La instancia correrá el mismo contenedor por lo que debe tener Docker instalada. Luego de consumir el mensaje SQS, la instancia iniciará el procesos ETL, de igual manera que la función Lambda. 

Para utilizar la aplicación como template se debe editar el archivo de configuración yaml en la carpeta respectiva. La aplicación puede ir a consultar secretos a Secrets Manager, en particular la clave del usuario de Redshift. Si dentro del archivo de configuración se cambia la clave por ```SecretsManager``` la aplicación irá a Secrets Manager a buscar la clave guardada.

Los datos de prueba son tales que coinciden con el set de datos de la competencia de Kaggle [Titanic - Machine Learning from Disaster](https://www.kaggle.com/competitions/titanic/data). La aplicación busca dos tablas dentro del clúster, y si no las encuentra las inicializa con un formato previo al inicio de su carga.

Finalmente, se ha disponibilizado un archivo ```DockerFile``` para correr la aplicación en un contenedor, y poder lanzar la aplicación desde una Lambda.