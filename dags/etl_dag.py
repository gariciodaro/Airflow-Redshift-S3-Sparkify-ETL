# -*- coding: utf-8 -*-
"""
Created on Sat Aug 22 2020
@author: gari.ciodaro.guerra
DAG of AirFlow to create ETL from S3 to
Redshift to populate Sparkify Database.
in Redshift. Run every hour. Backfill data since december
of 2019. 
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries
from airflow.contrib.hooks.aws_hook import AwsHook


#get AWS credentials from AirFlow webserver
aws_hook = AwsHook("aws_credentials")
credentials = aws_hook.get_credentials()

# Default parameters for operators in DAG
#prevent parallelized process with max_active_runs
# In case of failure, run 3 times with a time interval
# of 3 minutes.
default_args = {
    'owner': 'gari',
    'start_date':datetime(2019, 1, 12),
    'catchup': False,
    'depends_on_past':False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}
# Dag definition
# run DAG every hour.
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          max_active_runs=1,
          schedule_interval='@hourly'
        )

# Stagging tables from json on S3 to Redshift 
stage_events_to_redshift = StageToRedshiftOperator(
    table='staging_events',
    redshift_conn_id = 'redshift' ,
    s3_path= 's3://udacity-dend/log_data',
    s3_json_option='s3://udacity-dend/log_json_path.json',
    task_id='Stage_events',
    dag=dag,
    credentials=credentials
)

stage_songs_to_redshift = StageToRedshiftOperator(
    table='staging_songs',
    redshift_conn_id = 'redshift' ,
    s3_path= 's3://udacity-dend/song_data',
    task_id='Stage_songs',
    dag=dag,
    credentials=credentials
)

# Insert into Dimensions tables from staging tables
load_user_dimension_table = LoadDimensionOperator(
    table='users',
    redshift_conn_id = 'redshift',
    task_id='Load_user_dim_table',
    operation='truncate',
    dag=dag,
    insert_statement=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    table='songs',
    redshift_conn_id = 'redshift',
    task_id='Load_song_dim_table',
    operation='truncate',
    dag=dag,
    insert_statement=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    table='artists',
    redshift_conn_id = 'redshift',
    task_id='Load_artist_dim_table',
    operation='truncate',
    dag=dag,
    insert_statement=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    table='time',
    redshift_conn_id = 'redshift',
    task_id='Load_time_dim_table',
    operation='truncate',
    dag=dag,
    insert_statement=SqlQueries.time_table_insert
)

#  Insert into Fact songplays from staging tables
load_songplays_table = LoadFactOperator(
    table='songplays',
    redshift_conn_id = 'redshift',
    task_id='Load_songplays_fact_table',
    operation='truncate',
    dag=dag,
    insert_statement=SqlQueries.songplay_table_insert
)

# quality checks operator
dq_checks=[
{'check_sql':'SELECT COUNT(*) FROM {}' , 'expected_result':'result<0'}
]
run_quality_checks = DataQualityOperator(
    table_list=['songplays', 'songs', 'artists', 'users', 'time'],
    redshift_conn_id = 'redshift',
    task_id='Run_data_quality_checks',
    dq_checks=dq_checks,
    dag=dag
)

#Dummy Operators
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
end_operator   = DummyOperator(task_id='Stop_execution' ,  dag=dag)

# Graph definition

start_operator >> [stage_events_to_redshift,stage_songs_to_redshift]
[stage_events_to_redshift,stage_songs_to_redshift]>>load_songplays_table 
load_songplays_table >> [load_user_dimension_table, 
                         load_song_dimension_table, 
                         load_artist_dimension_table,
                         load_time_dimension_table]
[load_user_dimension_table,
 load_song_dimension_table,
 load_artist_dimension_table,
 load_time_dimension_table]>>run_quality_checks
run_quality_checks>>end_operator