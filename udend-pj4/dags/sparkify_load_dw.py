from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'ap3x',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG('sparkify_dag',
            default_args=default_args,
            description='Load and transform data in Redshift with Airflow',
            schedule_interval='0 * * * *')

start_task = DummyOperator(task_id='begin_execution',  dag=dag)

s3_bucket = "udacity-dend"
redshift_conn = "redshift"
aws_credentials= "aws_credentials"

staging_events_table = "staging_events"
stage_events_to_redshift_task = StageToRedshiftOperator(
    task_id='stage_events',
    dag=dag,
    table=staging_events_table,
    redshift_conn_id=redshift_conn,
    aws_credentials_id=aws_credentials,
    s3_bucket=s3_bucket,
    jsonPath = "log_json_path.json",
    s3_key="log_data",
    s3_region="us-west-2",
    format= "json"
)

staging_songs_table = "staging_songs"
stage_songs_to_redshift_task = StageToRedshiftOperator(
    task_id='stage_songs',
    dag=dag,
    table=staging_songs_table,
    redshift_conn_id=redshift_conn,
    aws_credentials_id=aws_credentials,
    s3_bucket=s3_bucket,
    jsonPath = "auto",
    s3_key="song_data",
    s3_region="us-west-2",
    format= "json"
)

songplays_fact_table = "songplays"
load_songplays_table_task = LoadFactOperator(
    task_id='load_songplays_fact_table',
    dag=dag,
    table=songplays_fact_table,
    sql_query=SqlQueries.songplay_table_insert,
    redshift_conn_id=redshift_conn
)

dim_user_table= "users"
append = False
load_user_dimension_table_task = LoadDimensionOperator(
    task_id='load_user_dim_table',
    dag=dag,
    append=append,
    table=dim_user_table,
    sql_query=SqlQueries.user_table_insert,
    redshift_conn_id=redshift_conn
)

dim_song_table="songs"
append = False
load_song_dimension_table_task = LoadDimensionOperator(
    task_id='load_song_dim_table',
    dag=dag,
    append=append,
    table=dim_song_table,
    sql_query=SqlQueries.song_table_insert,
    redshift_conn_id=redshift_conn    
)

dim_artist_table="artists"
append = False
load_artist_dimension_table_task = LoadDimensionOperator(
    task_id='load_artist_dim_table',
    dag=dag,
    append=append,
    table=dim_artist_table,
    sql_query=SqlQueries.artist_table_insert,
    redshift_conn_id=redshift_conn    
)

dim_time_table= "time"
append = False
load_time_dimension_table_task = LoadDimensionOperator(
    task_id='load_time_dim_table',
    dag=dag,
    append=append,
    table=dim_time_table,
    sql_query=SqlQueries.time_table_insert,
    redshift_conn_id=redshift_conn     
)

data_quality_table_list =[dim_user_table,dim_song_table,dim_artist_table,dim_time_table]
data_quality_task = DataQualityOperator(
    task_id='data_quality',
    dag=dag,
    redshift_conn_id=redshift_conn,
    tableList=data_quality_table_list
)

end_task = DummyOperator(task_id='stop_execution',  dag=dag)

start_task >> [stage_events_to_redshift_task, stage_songs_to_redshift_task]

[stage_events_to_redshift_task, stage_songs_to_redshift_task] >> load_songplays_table_task

load_songplays_table_task >> [load_song_dimension_table_task, load_user_dimension_table_task, 
                         load_artist_dimension_table_task, load_time_dimension_table_task]

[load_song_dimension_table_task, load_user_dimension_table_task, 
 load_artist_dimension_table_task, load_time_dimension_table_task] >> data_quality_task

data_quality_task >> end_task
