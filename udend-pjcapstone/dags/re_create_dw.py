"""
This module is used to drop and recreate tables at redShift.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from helpers.re_create_dw import ReCreateDWQueries

default_args = {
    'owner': 'ap3x',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

REDSHIFT_CONNECTION = 'redshift'

re_create_dw_dag = DAG('capstone.re_create_dw',
                      default_args=default_args,
                      description='(Re) Create tables at Redshift',
                      schedule_interval=None,
                      catchup=False)

start_task = DummyOperator(task_id='begin_execution',  dag=re_create_dw_dag)

drop_dim_time_task = PostgresOperator(task_id='drop_dim_time', 
                                              dag=re_create_dw_dag,
                                              postgres_conn_id=REDSHIFT_CONNECTION,
                                              sql=ReCreateDWQueries.drop_dim_time)

drop_dim_city_location_task = PostgresOperator(task_id='drop_dim_city_location', 
                                                 dag=re_create_dw_dag,
                                                 postgres_conn_id=REDSHIFT_CONNECTION,
                                                 sql=ReCreateDWQueries.drop_dim_city_location)

drop_dim_location_description_task = PostgresOperator(task_id='drop_dim_location_description', 
                                             dag=re_create_dw_dag,
                                             postgres_conn_id=REDSHIFT_CONNECTION,
                                             sql=ReCreateDWQueries.drop_dim_location_description)

drop_dim_crime_description_task = PostgresOperator(task_id='drop_dim_crime_description', 
                                            dag=re_create_dw_dag,
                                            postgres_conn_id=REDSHIFT_CONNECTION,
                                            sql=ReCreateDWQueries.drop_dim_crime_description)

drop_ft_crimes_task = PostgresOperator(task_id='drop_ft_crimes', 
                                            dag=re_create_dw_dag,
                                            postgres_conn_id=REDSHIFT_CONNECTION,
                                            sql=ReCreateDWQueries.drop_ft_crimes)

drop_staging_crimes_task = PostgresOperator(task_id='drop_staging_crimes', 
                                                      dag=re_create_dw_dag,
                                                      postgres_conn_id=REDSHIFT_CONNECTION,
                                                      sql=ReCreateDWQueries.drop_staging_crimes)

drop_staging_crime_descriptions_task = PostgresOperator(task_id='drop_staging_crime_descriptions', 
                                                     dag=re_create_dw_dag,
                                                     postgres_conn_id=REDSHIFT_CONNECTION,
                                                     sql=ReCreateDWQueries.drop_staging_crime_descriptions)

create_dim_time_task = PostgresOperator(task_id='create_dim_time', 
                                                dag=re_create_dw_dag,
                                                postgres_conn_id=REDSHIFT_CONNECTION,
                                                sql=ReCreateDWQueries.create_dim_time)

create_dim_crime_description_task = PostgresOperator(task_id='create_dim_crime_description', 
                                                   dag=re_create_dw_dag,
                                                   postgres_conn_id=REDSHIFT_CONNECTION,
                                                   sql=ReCreateDWQueries.create_dim_crime_description)

create_dim_city_location_task = PostgresOperator(task_id='create_dim_city_location', 
                                               dag=re_create_dw_dag,
                                               postgres_conn_id=REDSHIFT_CONNECTION,
                                               sql=ReCreateDWQueries.create_dim_city_location)

create_dim_location_description_task = PostgresOperator(task_id='create_dim_location_description', 
                                              dag=re_create_dw_dag,
                                              postgres_conn_id=REDSHIFT_CONNECTION,
                                              sql=ReCreateDWQueries.create_dim_location_description)

create_ft_crimes_task = PostgresOperator(task_id='create_ft_crimes', 
                                              dag=re_create_dw_dag,
                                              postgres_conn_id=REDSHIFT_CONNECTION,
                                              sql=ReCreateDWQueries.create_ft_crimes)

create_staging_crimes_task = PostgresOperator(task_id='create_staging_crimes', 
                                                        dag=re_create_dw_dag,
                                                        postgres_conn_id=REDSHIFT_CONNECTION,
                                                        sql=ReCreateDWQueries.create_staging_crimes)

create_staging_crime_descriptions_task = PostgresOperator(task_id='create_staging_crime_descriptions', 
                                                       dag=re_create_dw_dag,
                                                       postgres_conn_id=REDSHIFT_CONNECTION,
                                                       sql=ReCreateDWQueries.create_staging_crime_descriptions)

end_task = DummyOperator(task_id='stop_execution',  dag=re_create_dw_dag)


drop_dimensions_tasks = [drop_dim_time_task, drop_dim_city_location_task, 
                        drop_dim_location_description_task, drop_dim_crime_description_task]
create_dimensions_tasks = [create_dim_time_task, create_dim_crime_description_task, 
                            create_dim_city_location_task, create_dim_location_description_task]    

drop_stagings_tasks = [drop_staging_crime_descriptions_task, drop_staging_crimes_task]            
create_stagings_task = [create_staging_crimes_task, create_staging_crime_descriptions_task]


start_task >> drop_ft_crimes_task
start_task >> drop_stagings_tasks

drop_ft_crimes_task >> drop_dimensions_tasks
create_dimensions_tasks >> create_ft_crimes_task

create_ft_crimes_task >> end_task
create_stagings_task >> end_task

drop_dim_time_task >> create_dim_time_task
drop_dim_city_location_task >> create_dim_city_location_task
drop_dim_location_description_task >> create_dim_location_description_task
drop_dim_crime_description_task >> create_dim_crime_description_task
drop_staging_crimes_task >> create_staging_crimes_task
drop_staging_crime_descriptions_task >> create_staging_crime_descriptions_task
