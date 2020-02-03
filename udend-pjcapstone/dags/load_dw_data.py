from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import EtlQueries

default_args = {
    'owner': 'ap3x',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False
}

dag = DAG('capstone.load_dw_data',
            default_args=default_args,
            description='Load and transform data in Redshift with Airflow',
            schedule_interval=None)

start_task = DummyOperator(task_id='begin_execution',  dag=dag)

s3_bucket = 'ap3x-dend-capstone'
redshift_conn = 'redshift'

load_staging_crimes_to_redshift_task = StageToRedshiftOperator(
    task_id='load_staging_crimes',
    dag=dag,
    table='staging_crimes',
    redshift_conn_id=redshift_conn,
    redshift_arn_variable_name='redshift_role_arn',
    s3_bucket=s3_bucket,
    s3_key='crimes',
    s3_region='us-east-1',
    format='csv'
)

load_staging_crime_descriptions_to_redshift_task = StageToRedshiftOperator(
    task_id='load_staging_crime_descriptions',
    dag=dag,
    table='staging_crime_descriptions',
    redshift_conn_id=redshift_conn,
    redshift_arn_variable_name='redshift_role_arn',
    s3_bucket=s3_bucket,
    jsonPath = 'codes-jsonpath.json',
    s3_key='crime-codes',
    s3_region='us-east-1',
    format= 'json'
)

staging_data_quality_table_list =['staging_crimes', 'staging_crime_descriptions']
staging_data_quality_task = DataQualityOperator(
    task_id='staging_data_quality',
    dag=dag,
    redshift_conn_id=redshift_conn,
    tableList=staging_data_quality_table_list
)

load_dim_time_task = LoadDimensionOperator(
    task_id='load_dim_time',
    dag=dag,
    table='dim_time',
    sql_query=EtlQueries.dim_time_insert,
    redshift_conn_id=redshift_conn
)

load_dim_crime_description_task = LoadDimensionOperator(
    task_id='load_dim_crime_description',
    dag=dag,
    table='dim_crime_description',
    sql_query=EtlQueries.dim_crime_descriptions_insert,
    redshift_conn_id=redshift_conn    
)

load_dim_location_description_task = LoadDimensionOperator(
    task_id='load_dim_location_description',
    dag=dag,
    table='dim_location_description',
    sql_query=EtlQueries.dim_location_descriptions_insert,
    redshift_conn_id=redshift_conn    
)

load_dim_city_location_task = LoadDimensionOperator(
    task_id='load_dim_city_location',
    dag=dag,
    table='dim_city_location',
    sql_query=EtlQueries.dim_city_location_insert,
    redshift_conn_id=redshift_conn     
)

load_ft_crimes_task = LoadFactOperator(
    task_id='load_ft_crimes',
    dag=dag,
    table='ft_crimes',
    sql_query=EtlQueries.ft_crimes_insert,
    redshift_conn_id=redshift_conn
)

fact_data_quality_table_list =['ft_crimes']
fact_data_quality_task = DataQualityOperator(
    task_id='fact_data_quality',
    dag=dag,
    redshift_conn_id=redshift_conn,
    tableList=fact_data_quality_table_list
)

end_task = DummyOperator(task_id='stop_execution',  dag=dag)

load_to_s3_tasks = [load_staging_crimes_to_redshift_task, load_staging_crime_descriptions_to_redshift_task]
load_dimensions_tasks = [load_dim_time_task, load_dim_crime_description_task,
                    load_dim_location_description_task, load_dim_city_location_task]

start_task >> load_to_s3_tasks
load_to_s3_tasks >> staging_data_quality_task
staging_data_quality_task >> load_dimensions_tasks
load_dimensions_tasks >> load_ft_crimes_task
load_ft_crimes_task >> fact_data_quality_task
fact_data_quality_task >> end_task
