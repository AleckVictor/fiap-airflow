FROM apache/airflow:2.8.1

# Usa o usuário airflow desde o início
USER airflow

# Instala o pacote oracledb com pip
RUN pip install oracledb
