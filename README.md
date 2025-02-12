# datalakeP
Proyecto de manejo de datos de práctica

Este es un proyecto de prueba que carga datos de un archivo Excel con servicios AWS, desde un bucket S3 a un clúster Redshift. Cuando un archivo es cargado en el bucket, S3 envía un mensaje a una cola SQS que la aplicación consume. 

Esta aplicación está diseñada y probada sobre una arquitectura no segura en la que tanto el clúster Redshift, como la instancia EC2 en la que corren están expuestos a Internet. Sin embargo, con modificaciones menores se puede utilizar una arquitectura más segura con el clúster en una VPC con una subred no expuesta. Además, si dentro del archivo de configuración se cambia la clave por ```SecretsManager``` la aplicación irá a Secrets Manager a buscar la clave guardada.

Los datos de prueba son tales que coinciden con el set de datos de la competencia de Kaggle [Titanic - Machine Learning from Disaster](https://www.kaggle.com/competitions/titanic/data). La aplicación busca dos tablas dentro del clúster, y si no las encuentra las inicializa con un formato previo al inicio de su carga.

Finalmente, se ha disponibilizado un archivo ```DockerFile``` para correr la aplicación en un contenedor, y poder lanzar la aplicación desde una Lambda.