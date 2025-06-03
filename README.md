## Solución Propuesta

Este repositorio presenta una solución completa al desafío técnico de un Ingeniero de Datos, ambientado en un entorno de simulación profesional. La arquitectura implementada emula un ecosistema productivo compuesto por un Data Warehouse modelado en esquema **copo de nieve**, y un pipeline ETL modular construido en **PySpark**, ejecutado dentro de un entorno **Dockerizado**.

### Contexto del entorno Alegra

El entorno simula una arquitectura tipo **Lakehouse**, con zonas diferenciadas de almacenamiento (`bronze`, `silver`, `gold`) y un contenedor de **SQL Server** que representa el sistema transaccional OLTP.

Los datos de entrada no provienen directamente del contenedor SQL, sino que se alojan inicialmente en la carpeta `data_source/`. Desde allí:

1. Se cargan sin transformación al área `bronze`, respetando su estructura original.
2. Se procesan en `silver` mediante limpieza, normalización y modelado conforme al esquema **copo de nieve**.
3. Finalmente, en `gold`, se generan las vistas analíticas o tablas agregadas necesarias para responder las **métricas del negocio** solicitadas en la prueba.

Este enfoque promueve una separación clara entre los niveles de calidad de datos, garantiza trazabilidad, y permite escalabilidad futura al integrarse fácilmente con catálogos externos, motores SQL, o herramientas BI.

## Requisitos del entorno

* Python 3.10+
* pip
* Virtualenv (`python -m venv`)
* Docker (para SQL Server ficticio)
* PySpark (usado en notebooks)
* JupyterLab o VSCode
* java

## Configuración del entorno

1. Clonar el repositorio:

   * `git clone https://github.com/usuario/data-engineer-health-assessment.git`
   * `cd data-engineer-health-assessment`
2. Crear y activar el entorno virtual:

   * `python -m venv venv`
   * `source venv/bin/activate  # Linux/Mac`
3. Instalar dependencias:

   * `pip install -r requirements.txt `
4. Instalá OpenJDK 11 (es el más recomendado para Spark 3.x):

   * `sudo apt update`
   * `sudo apt install openjdk-11-jdk -y`
5. Ubicar la ruta de Java:

   * `readlink -f $(which java)`
   * Esto dará un path que hay que cargar (paso 6)
6. Añador en el archivo `.bashrc` (Linux) o `.zshrc` (Powershell) el `JAVA_HOME` (path obtenido en el paso anterior).

   * `nano ~/.zshrc`
7. Añadir al archivo al final la variable:

   * `export JAVA_HOME="path obtenido en el paso anterior"  `
   * `export PATH=$JAVA_HOME/bin:$PATH `
8. Aplicar los cambios:
   `source ~/.zshrc`
9. test: `python -c "from pyspark.sql import SparkSession; print(SparkSession.builder.master('local[*]').getOrCreate().version)"`

   Si todo está ok veremos algo así:
   `25/04/23 00:55:28 WARN Utils: Your hostname, NicoNavarro resolves to a loopback address: 127.0.1.1; using 10.255.255.254 instead (on interface lo) `
   `25/04/23 00:55:28 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address  Setting default log level to "WARN".  To adjust logging level use sc.setLogLevel(newLevel).  For SparkR, use setLogLevel(newLevel). 25/04/23 00:55:29 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable  3.4.1`
10. (Opcional) Crear archivo `.env` basado en el `.env.example` si usas variables de entorno para configurar rutas, puertos, etc.
11. Iniciar el contenedor de SQL Server:
    `cd sql_server docker-compose up -d`
12. Levantar los notebooks para desarrollo:
    `jupyter lab  # o usar VSCode con extensión de Jupyter`

## Modelo de Datos Propuesto

El modelo implementado sigue una estructura copo de nieve (snowflake), adecuada para entornos donde se requiere normalización de las dimensiones, permitiendo mantener los datos limpios, estructurados y fácilmente escalables.

El modelo está centrado en la tabla de hechos fact_invoices, que captura la granularidad de cada línea de factura (producto vendido por cliente en una fecha específica), y se relaciona con varias tablas de dimensión: cliente, producto, tiempo y sus respectivas jerarquías normalizadas.

### Diagrama del Modelo

![1748898058963](image/README/1748898058963.png)

#### Tabla de Hechos

fact_invoices
Columna	Tipo	Descripción
id	INT	Identificador de la transacción (PK)
id_customer	INT	Cliente que realizó la compra (FK)
id_product	INT	Producto vendido (FK)
id_date	INT	Fecha de la factura (FK a dim_time)
quantity	INT	Número de unidades vendidas
total_amount	FLOAT	Valor total de la transacción
created_at	DATETIME	Fecha de creación del registro
updated_at	DATETIME	Última actualización
deleted_at	DATETIME	Eliminación lógica (si aplica)

#### Tablas de Dimensión

dim_customer
Columna	Tipo	Descripción
id	INT	Identificador del cliente (PK)
name	TEXT	Nombre completo
id_location	INT	FK a dim_location
id_segment	INT	FK a dim_segment
created_at	DATETIME	Fecha de creación
updated_at	DATETIME	Fecha de actualización
deleted_at	DATETIME	Eliminación lógica

dim_location
Columna	Tipo	Descripción
id	INT	Identificador de región/localidad
name	TEXT	Nombre de la región
created_at	DATETIME	Fecha de creación
updated_at	DATETIME	Fecha de actualización
deleted_at	DATETIME	Eliminación lógica

dim_segment
Columna	Tipo	Descripción
id	INT	ID del segmento de cliente
description	TEXT	Descripción del segmento
created_at	DATETIME	Fecha de creación
updated_at	DATETIME	Fecha de actualización
deleted_at	DATETIME	Eliminación lógica

dim_product
Columna	Tipo	Descripción
id	INT	Identificador del producto
name	TEXT	Nombre del producto
id_category	INT	FK a dim_product_category
price	FLOAT	Precio unitario del producto
created_at	DATETIME	Fecha de creación
updated_at	DATETIME	Fecha de actualización
deleted_at	DATETIME	Eliminación lógica

dim_product_category
Columna	Tipo	Descripción
id	INT	ID de la categoría
name	TEXT	Nombre de la categoría
created_at	DATETIME	Fecha de creación
updated_at	DATETIME	Fecha de actualización
deleted_at	DATETIME	Eliminación lógica

dim_time
Columna	Tipo	Descripción
date_id	INT	Identificador de la fecha (PK)
full_date	DATE	Fecha completa
day	INT	Día del mes
month	INT	Mes numérico
month_name	TEXT	Nombre del mes
quarter	INT	Trimestre (1-4)
year	INT	Año calendario
attribute_8	TEXT	(Campo reservado para extensiones)

#### Granularidad

La granularidad de la tabla de hechos fact_invoices es a nivel de línea de venta por producto, cliente y fecha, permitiendo analizar patrones de consumo detallados, por segmento, por categoría y por región.
