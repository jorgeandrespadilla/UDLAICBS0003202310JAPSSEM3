# Implementación Bodegas de Datos

*Jorge Andrés Padilla*

## Instalación de paquetes

Para instalar los paquetes necesarios usados por Python, se debe ejecutar el comando `pip install -r requirements.txt`

## Actualización de archivos de configuración

Modificar los archivos de configuración ubicados en el directorio `config` de acuerdo a la configuración de conexión a la base de datos.

El archivo `data.properties` contiene la configuración de acceso a los archivos de datos CSV, y el archivo `db.properties` contiene la configuración de conexión a la base de datos.

## Evidencias

Las evidencias se encuentran en el directorio `evidencesSem3`.

El archivo `sor-model.png` contiene el modelo de datos de la base SOR, y el archivo `stg-model.png` contiene el modelo de datos de la base Staging. Adicionalmente, el directorio `evidencesSem3/record-cound` contiene las imágenes con el conteo de registros de cada tabla de la base de Staging.

## Ejecución de pruebas

Para correr la extracción de datos, se debe ejecutar el comando: `python py_startup.py`
