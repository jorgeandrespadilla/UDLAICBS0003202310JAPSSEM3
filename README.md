# Implementación Bodegas de Datos

*Jorge Andrés Padilla*

## Descripción

Este repositorio contiene el código fuente usado para el ejercicio de implementación de Bodegas de Datos. Para ello, se utilizó Python 3.9 y MySQL 8.0 para el almacenamiento de datos.

## Instalación de paquetes

Para instalar los paquetes necesarios usados por Python, se debe ejecutar el comando `pip install -r requirements.txt`

## Actualización de archivos de configuración

Modificar los archivos de configuración ubicados en el directorio `config` de acuerdo a la configuración de conexión a la base de datos.

El archivo `data.properties` contiene la configuración de acceso a los archivos de datos CSV, y el archivo `db.properties` contiene la configuración de conexión a la base de datos.

## Evidencias

Las evidencias se encuentran en el directorio `evidencesSem3`.

El archivo `sor-model.png` contiene el modelo de datos de la base SOR, y el archivo `stg-model.png` contiene el modelo de datos de la base Staging. Adicionalmente, el directorio `evidencesSem3/record-cound` contiene las imágenes con el conteo de registros de cada tabla de la base de Staging.

## Ejecución de scripts SQL

Los scripts SQL se encuentran ubicados en el directorio `sql`, y se deben ejecutar en una base de datos MySQL en el siguiente orden:
1. `initialization.sql` (inicialización de esquemas)
2. `stg-tables.sql` (creación de tablas de la base de Staging)
3. `tra-tables.sql` (creación de tablas de la base de Transformación)
4. `sor-tables.sql` (creación de tablas de la base SOR)

## Ejecución de pruebas

Para correr la extracción de datos, se debe ejecutar el comando: `python py_startup.py`
