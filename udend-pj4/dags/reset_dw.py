"""
This module is used to drop and recreate tables at redShift.
"""
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from helpers.reset_dw_queries import ResetDWQueries

default_args = {
    'owner': 'ap3x',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

REDSHIFT_CONNECTION = 'redshift'

reset_dw_dag = DAG('reset_dw_dag',
                      default_args=default_args,
                      description='Drop and recreate tables at Redshift',
                      schedule_interval=None)

start_task = DummyOperator(task_id='begin_execution',  dag=reset_dw_dag)

drop_table_artist_task = PostgresOperator(task_id='drop_table_artist', 
                                              dag=reset_dw_dag,
                                              postgres_conn_id=REDSHIFT_CONNECTION,
                                              sql=ResetDWQueries.drop_table_artist)

drop_table_songplays_task = PostgresOperator(task_id='drop_table_songplays', 
                                                 dag=reset_dw_dag,
                                                 postgres_conn_id=REDSHIFT_CONNECTION,
                                                 sql=ResetDWQueries.drop_table_songplays)

drop_table_songs_task = PostgresOperator(task_id='drop_table_songs', 
                                             dag=reset_dw_dag,
                                             postgres_conn_id=REDSHIFT_CONNECTION,
                                             sql=ResetDWQueries.drop_table_songs)

drop_table_time_task = PostgresOperator(task_id='drop_table_time', 
                                            dag=reset_dw_dag,
                                            postgres_conn_id=REDSHIFT_CONNECTION,
                                            sql=ResetDWQueries.drop_table_time)

drop_table_user_task = PostgresOperator(task_id='drop_table_user', 
                                            dag=reset_dw_dag,
                                            postgres_conn_id=REDSHIFT_CONNECTION,
                                            sql=ResetDWQueries.drop_table_users)

drop_table_staging_events_task = PostgresOperator(task_id='drop_table_staging_events', 
                                                      dag=reset_dw_dag,
                                                      postgres_conn_id=REDSHIFT_CONNECTION,
                                                      sql=ResetDWQueries.drop_table_staging_events)

drop_table_staging_songs_task = PostgresOperator(task_id='drop_table_staging_songs', 
                                                     dag=reset_dw_dag,
                                                     postgres_conn_id=REDSHIFT_CONNECTION,
                                                     sql=ResetDWQueries.drop_table_staging_songs)

create_table_artist_task = PostgresOperator(task_id='create_table_artist', 
                                                dag=reset_dw_dag,
                                                postgres_conn_id=REDSHIFT_CONNECTION,
                                                sql=ResetDWQueries.create_table_artists)

create_table_songplays_task = PostgresOperator(task_id='create_table_songplays', 
                                                   dag=reset_dw_dag,
                                                   postgres_conn_id=REDSHIFT_CONNECTION,
                                                   sql=ResetDWQueries.create_table_songplays)

create_table_songs_task = PostgresOperator(task_id='create_table_songs', 
                                               dag=reset_dw_dag,
                                               postgres_conn_id=REDSHIFT_CONNECTION,
                                               sql=ResetDWQueries.create_table_songs)

create_table_time_task = PostgresOperator(task_id='create_table_time', 
                                              dag=reset_dw_dag,
                                              postgres_conn_id=REDSHIFT_CONNECTION,
                                              sql=ResetDWQueries.create_table_time)

create_table_user_task = PostgresOperator(task_id='create_table_user', 
                                              dag=reset_dw_dag,
                                              postgres_conn_id=REDSHIFT_CONNECTION,
                                              sql=ResetDWQueries.create_table_users)

create_table_staging_events_task = PostgresOperator(task_id='create_table_staging_events', 
                                                        dag=reset_dw_dag,
                                                        postgres_conn_id=REDSHIFT_CONNECTION,
                                                        sql=ResetDWQueries.create_table_staging_events)

create_table_staging_songs_task = PostgresOperator(task_id='create_table_staging_songs', 
                                                       dag=reset_dw_dag,
                                                       postgres_conn_id=REDSHIFT_CONNECTION,
                                                       sql=ResetDWQueries.create_table_staging_songs)

end_task = DummyOperator(task_id='stop_execution',  dag=reset_dw_dag)

start_task >> [drop_table_artist_task, drop_table_songplays_task, drop_table_songs_task,
                  drop_table_time_task, drop_table_user_task, drop_table_staging_events_task,
                  drop_table_staging_songs_task]

end_task << [create_table_artist_task, create_table_songplays_task, create_table_songs_task,
                 create_table_time_task, create_table_user_task, create_table_staging_events_task,
                 create_table_staging_songs_task]

drop_table_artist_task >> create_table_artist_task
drop_table_songplays_task >> create_table_songplays_task
drop_table_songs_task >> create_table_songs_task
drop_table_time_task >> create_table_time_task
drop_table_user_task >> create_table_user_task
drop_table_staging_events_task >> create_table_staging_events_task
drop_table_staging_songs_task >> create_table_staging_songs_task
