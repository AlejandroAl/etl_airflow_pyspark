# etl_airflow_pyspark

La infrestructura se tomó como base la solución de los autores Matthieu "Puckel_" Roisil y Marc Lamberti
https://github.com/marclamberti
https://github.com/puckel/docker-airflow

## Pre configuración

Para generar los datos que utilizará el pipeline se utilizó el script:

./scripts_python_extras/script_genera_data.py

```
python ./scripts_python_extras/script_genera_data.py
```

NOTA: antes de ejecutar el script es necesario crear las siguientes variables de enterno.

VENTAS_ERP_DIR -> indica el lugar donde se encuentran los archivos CSV entregados en el examen (correspondientes de tortas de tamales inc)
PATH_TEINVENTO -> indica el lugar donde se encuentran los archivos CSV entregados en el examen (correspondientes de teInvento inc)
PATH_TEINVENTO_DATALAKE --> Indica el lugar donde se generan los archivos nuevos divididos por fecha, AAAAMM para teinvento.inc

# Deploy

Requerimientos:

Encontrarse en un entorno linux.

Instalación de docker, agregar su usario al grupo de docker para le ejecución de los comandos.

[link instalación docker](https://docs.docker.com/engine/install/)

Instalación de docker-compose 

[link instalación docker-compose](https://docs.docker.com/compose/install/)

Tener inslalado git.

Pasos para la validación desplegar los servicios:

1. Realizar el git clone del repository

```
git clone https://github.com/AlejandroAl/etl_airflow_python.git
```

2. Ubicarse en la carpeta raiz del proyecto 

```
cd {ubicación descarga}/etl_airflow_python
```

3. Ejecutar el script setup.sh

```
. script/setup.sh 
```

4. Ejecutar el comando 

```
docker-compose up -d --build

```

5. Los servicios tardan un poco el levantar, podemos visualizar el avance mediante el comando:

```
docker logs -f pipeline_webserver_1
```

El comando anterior mostrará el log del servicio que se encuentra levantandose, podria apaecer valors mensajes como los siguientes:

waiting for postgres 4/100

Los cuales indican que aun faltan por levantar los servicios de las base de datos.

Cuando se muestre en la terminal el logo de airflow, el servicio a finalizado de levantar.

Una vez que se visualice este contenido, esposible navegar a través de un explorador web la consola de airflow.

http://localhost:8081

Podremos observar el pipeline el cual esta con un crontab de ejecución  0 0 1 * *, este indica que se ejecutará los dias primero de cada mes, el desarrollo esta adaptado para tomar el mes previo para realizar el ETL y agregar la nyeva información de la base de datos.

![Pipeline sobre airflow](https://github.com/AlejandroAl/etl_airflow_python/blob/main/imagenes/scheduler_pipeline.png)

EL pipeline esta diseñado con 5 pasos:

El primero es para crear nuestra base de datos **venta_tortas**.
El segundo al 4 paso esta diseñado para generar las **dimensiones** utilizando la información compartida por teInvento.inc y un proceso en python para generar la dimension de tiempo.
El paso 5 es el encargado de ejecutar los siguientes pasos:

  1. Consumir la información de la carpeta correspondiente a su mes, proveniente de ventas mensuales Tamales Inc
  2. Consumir la información de la carpeta correspondiente a su mes, proveniente de ventas mensuales teInvento Inc
  3. Se obtiene los identificadores a los datos de tamales inc para generar los datos en forma de estrella.
  4. Se calculan las metricas.
  5. Para obtener los acumulados y el porcentaje se obtine los datos de las metricas previas, si es el mes de enero se omite para iniciar el año con un acumlado del primer mes.
  6. Se obtienen los datos finales y son alamacenados en postgres en la tabla llamada venta.


NOTA: El desarrollo de este pipeline esta simulado para ejecutar 4 tareeas simulando los meses de enero, febrero, marzo y abril.

 ![Pipeline completed](https://github.com/AlejandroAl/etl_airflow_python/blob/main/imagenes/dag_airflow_completed.png) 


Al finalizar el pipeline nos podremos conectar con un visualizador de base de datos utilizando los siguiente atributos:

databse: ventas_tortas
user: airflow
password: airflow
host: localhost
port: 5432

![Data base ventas_tortas](https://github.com/AlejandroAl/etl_airflow_python/blob/main/imagenes/database.png) 

Al finalizar de realizar la revisión es necesario bajar los servicios con el siguiente comando:

```
docker-compose down -v
```
