from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import sys
import os

# Agregar el directorio spark al path
sys.path.append('/opt/airflow/spark')

# Configuración por defecto del DAG
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Definición del DAG
dag = DAG(
    'data_pipeline_etl',
    default_args=default_args,
    description='Pipeline ETL completo para datos de ventas',
    schedule_interval='@daily',
    max_active_runs=1,
    tags=['etl', 'sales', 'data-quality']
)

def run_data_ingestion():
    """Ejecuta el proceso de ingesta de datos"""
    from data_ingestion import DataIngestion
    
    ingestion = DataIngestion()
    result = ingestion.ingest_all_sources()
    
    if not result['success']:
        raise Exception(f"Error en ingesta: {result['message']}")
    
    return result['records_ingested']

def run_data_cleaning():
    """Ejecuta el proceso de limpieza de datos"""
    from data_cleaning import DataCleaner
    
    cleaner = DataCleaner()
    result = cleaner.clean_all_datasets()
    
    if not result['success']:
        raise Exception(f"Error en limpieza: {result['message']}")
    
    return result['records_cleaned']

def run_data_transformation():
    """Ejecuta las transformaciones de datos"""
    from data_transformation import DataTransformer
    
    transformer = DataTransformer()
    result = transformer.transform_sales_data()
    
    if not result['success']:
        raise Exception(f"Error en transformación: {result['message']}")
    
    return result['records_transformed']

def run_data_loading():
    """Carga los datos al Data Warehouse"""
    from data_loader import DataLoader
    
    loader = DataLoader()
    result = loader.load_to_warehouse()
    
    if not result['success']:
        raise Exception(f"Error en carga: {result['message']}")
    
    return result['records_loaded']

def run_data_quality_checks():
    """Ejecuta validaciones de calidad de datos"""
    from monitoring.data_quality_monitor import DataQualityMonitor
    
    monitor = DataQualityMonitor()
    result = monitor.run_all_checks()
    
    if result['critical_failures'] > 0:
        raise Exception(f"Fallos críticos en calidad: {result['critical_failures']}")
    
    return result

def send_completion_notification():
    """Envía notificación de finalización"""
    from monitoring.alert_system import AlertSystem
    
    alert = AlertSystem()
    alert.send_success_notification()

# Definición de tareas
task_data_ingestion = PythonOperator(
    task_id='data_ingestion',
    python_callable=run_data_ingestion,
    dag=dag
)

task_data_cleaning = PythonOperator(
    task_id='data_cleaning',
    python_callable=run_data_cleaning,
    dag=dag
)

task_data_transformation = PythonOperator(
    task_id='data_transformation',
    python_callable=run_data_transformation,
    dag=dag
)

task_data_loading = PythonOperator(
    task_id='data_loading',
    python_callable=run_data_loading,
    dag=dag
)

task_data_quality = PythonOperator(
    task_id='data_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag
)

task_create_daily_summary = PostgresOperator(
    task_id='create_daily_summary',
    postgres_conn_id='postgres_default',
    sql="""
    INSERT INTO daily_sales_summary (
        summary_date, total_sales_amount, total_transactions,
        unique_customers, avg_transaction_value, top_product_category
    )
    SELECT 
        CURRENT_DATE as summary_date,
        SUM(total_amount) as total_sales_amount,
        COUNT(*) as total_transactions,
        COUNT(DISTINCT customer_id) as unique_customers,
        AVG(total_amount) as avg_transaction_value,
        (SELECT dp.category 
         FROM fact_sales fs2 
         JOIN dim_products dp ON fs2.product_id = dp.product_id 
         WHERE fs2.sale_date = CURRENT_DATE 
         GROUP BY dp.category 
         ORDER BY SUM(fs2.total_amount) DESC 
         LIMIT 1) as top_product_category
    FROM fact_sales 
    WHERE sale_date = CURRENT_DATE
    ON CONFLICT (summary_date) DO UPDATE SET
        total_sales_amount = EXCLUDED.total_sales_amount,
        total_transactions = EXCLUDED.total_transactions,
        unique_customers = EXCLUDED.unique_customers,
        avg_transaction_value = EXCLUDED.avg_transaction_value,
        top_product_category = EXCLUDED.top_product_category;
    """,
    dag=dag
)

task_notification = PythonOperator(
    task_id='send_notification',
    python_callable=send_completion_notification,
    dag=dag
)

# Definición de dependencias
task_data_ingestion >> task_data_cleaning >> task_data_transformation >> task_data_loading >> task_data_quality >> task_create_daily_summary >> task_notification