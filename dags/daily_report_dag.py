# dags/daily_report_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import pandas as pd

# ฟังก์ชันสำหรับสร้างรายงาน
def generate_daily_report(**context):
    """
    สร้างรายงานประจำวัน
    context: จะมีข้อมูลที่ Airflow ส่งมาให้ เช่น execution_date
    """
    # จำลองการดึงข้อมูลและสร้างรายงาน
    report_date = context['execution_date']
    
    # สร้างข้อมูลตัวอย่าง (ในงานจริงคุณจะดึงข้อมูลจากฐานข้อมูลหรือ API)
    report_content = f"รายงานประจำวันที่ {report_date}\n"
    report_content += "1. ยอดขาย: 150,000 บาท\n"
    report_content += "2. จำนวนลูกค้าใหม่: 25 คน\n"
    
    # ส่งข้อมูลไปยัง task ถัดไป
    context['task_instance'].xcom_push(key='report_content', value=report_content)
    
    return "สร้างรายงานเสร็จสิ้น"

# กำหนดค่าเริ่มต้นสำหรับ DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,     # ไม่ต้องรอ task ก่อนหน้าสำเร็จ
    'start_date': datetime(2024, 1, 1),
    'email': ['your-email@company.com'],
    'email_on_failure': True,     # ส่งอีเมลเมื่อ task ล้มเหลว
    'email_on_retry': False,      # ไม่ต้องส่งอีเมลเมื่อ retry
    'retries': 1,                 # จำนวนครั้งที่จะ retry เมื่อล้มเหลว
    'retry_delay': timedelta(minutes=5),  # ระยะเวลารอก่อน retry
}

# สร้าง DAG
with DAG(
    'daily_report',              # ชื่อ DAG (ต้องไม่ซ้ำกับ DAG อื่น)
    default_args=default_args,    # ใช้ค่า default ที่กำหนดไว้ข้างบน
    description='สร้างและส่งรายงานประจำวัน',
    schedule_interval='0 7 * * *',  # เวลา 7:00 น. ของทุกวัน
    catchup=False                 # ไม่ต้องรันย้อนหลัง
) as dag:

    # Task ที่ 1: สร้างรายงาน
    create_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_daily_report,
        provide_context=True,     # ให้ Airflow ส่ง context เข้ามาใน function
    )

    # Task ที่ 2: ส่งอีเมลรายงาน
    send_email = EmailOperator(
        task_id='send_email_report',
        to=['recipient@company.com'],
        subject='รายงานประจำวัน {{ ds }}',  # ds คือ execution date ในรูปแบบ YYYY-MM-DD
        html_content="{{ task_instance.xcom_pull(task_ids='generate_report', key='report_content') | replace('\n', '<br/>') }}",
    )

    # กำหนดลำดับการทำงาน: สร้างรายงาน -> ส่งอีเมล
    create_report >> send_email