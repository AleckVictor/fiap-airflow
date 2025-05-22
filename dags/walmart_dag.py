from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import oracledb
import os

# Função principal
def carregar_dados_oracle():
    # Caminho do arquivo local (já extraído manualmente)
    file_path = "/opt/airflow/dags/walmart_dataset/Walmart_customer_purchases.csv"

    # Lê o arquivo
    df = pd.read_csv(file_path)

    # Conversão da data
    df['Purchase_Date'] = pd.to_datetime(df['Purchase_Date'], errors='coerce')

    # Copia apenas as colunas relevantes
    raw_data = df[['Customer_ID', 'Age', 'Gender', 'City', 'Category', 'Product_Name',
                   'Purchase_Date', 'Purchase_Amount', 'Payment_Method',
                   'Discount_Applied', 'Rating', 'Repeat_Customer']].copy()
    
    user = os.getenv("ORACLE_USER")
    password = os.getenv("ORACLE_PASSWORD")
    dsn = os.getenv("ORACLE_DSN")

    # Conecta no banco Oracle
    conn = oracledb.connect(
        user=user,
        password=password,
        dsn=dsn
    )

    print("USER:", os.getenv("ORACLE_USER"))
    print("PASS:", os.getenv("ORACLE_PASSWORD"))
    print("DSN:", os.getenv("ORACLE_DSN"))

    cur = conn.cursor()

    # Cria a tabela se não existir
    create_table_sql = """
    DECLARE
        nCount NUMBER;
    BEGIN
        SELECT COUNT(*) INTO nCount
        FROM ALL_TABLES
        WHERE TABLE_NAME = 'AIRFLOW_TESTE' AND OWNER = 'RM555705';

        IF nCount = 0 THEN
            EXECUTE IMMEDIATE '
                CREATE TABLE RM555705.AIRFLOW_TESTE (
                    CUSTOMER_ID        VARCHAR2(50),
                    AGE                NUMBER(3),
                    GENDER             VARCHAR2(10),
                    CITY               VARCHAR2(100),
                    CATEGORY           VARCHAR2(100),
                    PRODUCT_NAME       VARCHAR2(200),
                    PURCHASE_DATE      DATE,
                    PURCHASE_AMOUNT    NUMBER(10,2),
                    PAYMENT_METHOD     VARCHAR2(50),
                    DISCOUNT_APPLIED   VARCHAR2(10),
                    RATING             NUMBER(2),
                    REPEAT_CUSTOMER    VARCHAR2(10)
                )';
        END IF;
    END;
    """
    cur.execute(create_table_sql)
    conn.commit()

    # Insere os dados
    cur.executemany("""
        INSERT INTO RM555705.AIRFLOW_TESTE (
            CUSTOMER_ID, AGE, GENDER, CITY, CATEGORY, PRODUCT_NAME,
            PURCHASE_DATE, PURCHASE_AMOUNT, PAYMENT_METHOD,
            DISCOUNT_APPLIED, RATING, REPEAT_CUSTOMER)
        VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12)
    """, raw_data.values.tolist())

    conn.commit()
    conn.close()


# DAG
with DAG(
    dag_id="walmart_carregar_dados_oracle",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once",  # Roda apenas uma vez
    catchup=False,
    tags=["walmart", "oracle"]
) as dag:
    tarefa = PythonOperator(
        task_id="carregar_dados_oracle",
        python_callable=carregar_dados_oracle
    )
