# dags/etl_analysis_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime, timedelta
import json
import pandas as pd

# ฟังก์ชันสำหรับประมวลผลข้อมูล
def process_data(**context):
    """
    ประมวลผลข้อมูลที่ได้จาก API และ Database
    """
    # ดึงข้อมูลที่ได้จาก API task ก่อนหน้า
    api_data = context['task_instance'].xcom_pull(task_ids='fetch_api_data')
    
    # แปลงข้อมูลเป็น DataFrame
    df = pd.DataFrame(json.loads(api_data))
    
    # ทำการวิเคราะห์ข้อมูล
    summary = df.groupby('category')['amount'].sum().reset_index()
    
    # เก็บผลลัพธ์ไว้ใช้ใน task ถัดไป
    context['task_instance'].xcom_push(key='summary', value=summary.to_dict())
    
    return "ประมวลผลข้อมูลเสร็จสิ้น"

# กำหนดค่าเริ่มต้นสำหรับ DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['data_team@company.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# สร้าง DAG
with DAG(
    'etl_analysis',
    default_args=default_args,
    description='ETL และวิเคราะห์ข้อมูลจากหลายแหล่ง',
    schedule_interval='0 1 * * *',  # เวลา 01:00 น. ของทุกวัน
    catchup=False
) as dag:

    # Task 1: ดึงข้อมูลจาก Database
    extract_from_db = PostgresOperator(
        task_id='extract_from_database',
        postgres_conn_id='postgres_db',  # ต้องตั้งค่า connection ใน Airflow UI ก่อน
        sql="""
            SELECT *
            FROM sales
            WHERE date = '{{ ds }}'
        """
    )

    # Task 2: ดึงข้อมูลจาก API
    fetch_api = SimpleHttpOperator(
        task_id='fetch_api_data',
        http_conn_id='api_server',  # ต้องตั้งค่า connection ใน Airflow UI ก่อน
        endpoint='/sales/daily',
        method='GET',
        headers={"Content-Type": "application/json"},
        response_filter=lambda response: response.json()
    )

    # Task 3: ประมวลผลข้อมูล
    process = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        provide_context=True,
    )

    # Task 4: บันทึกผลลัพธ์กลับลง Database
    save_results = PostgresOperator(
        task_id='save_results',
        postgres_conn_id='postgres_db',
        sql="""
            INSERT INTO sales_summary (date, category, total_amount)
            VALUES (
                '{{ ds }}',
                '{{ task_instance.xcom_pull(task_ids='process_data', key='summary')['category'] }}',
                {{ task_instance.xcom_pull(task_ids='process_data', key='summary')['amount'] }}
            )
        """
    )

    # กำหนดลำดับการทำงาน
    [extract_from_db, fetch_api] >> process >> save_results