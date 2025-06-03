FROM jupyter/pyspark-notebook:latest

USER root

#  instalar dependencias base + driver ODBC 
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl gnupg unixodbc-dev && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list \
        > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y --allow-downgrades --allow-change-held-packages \
        msodbcsql17 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER jovyan

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN python -m ipykernel install --user --name pyspark --display-name "PySpark"

# Driver JDBC para Spark
COPY spark_docker/jupyter/drivers/mssql-jdbc-12.10.0.jre11.jar /usr/local/spark/jars/
