from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, PostgresOperator)

from helpers import SqlQueries
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

"""
    This module to define a DAG and its tasks using 
    StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator
    which defined in /plugins/operators directory.
    
    Steps will be performed:
    
    Define default_args which consists arguments will be used to define DAG and tasks.
    
    Create a DAG which will use some arguments from default_args, such as: 
    start_date, email_on_retry, depends_on_past, retries, retry_delay, catchup.
    
    Then define tasks using custom operators:
    
        - start_operator, end_operator tasks: to start and finish the dag.
        - stage_events_to_redshift task: to loading data to event staging table.
        - stage_songs_to_redshift task: to loading data into song staging table.        
        - load_songplays_table task: to loading data to SONGPLAYS table.
        - load_user_dimension_table task: to loading data to USERS table.
        - load_song_dimension_table task: to loading data to SONGS table.
        - load_time_dimension_table task: to loading data to TIME table.
        - load_artist_dimension_table task: to loading data to ARTISTS table. 
        - run_quality_checks task: to perform data validation.
    
    Finally, set the dependencies between tasks to build the flow.
    
"""

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 5),
    'email_on_retry': False,
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'redshift_conn_id': 'redshift',
    'aws_credential_id': 'aws_credentials'
}

dag = DAG('udac_dag',
          default_args = default_args,
          description = 'Load and transform data in Redshift with Airflow',
          schedule_interval = '@hourly',
          max_active_runs = 6
        )

start_operator = DummyOperator(task_id='Begin_execution', default_args = default_args, dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table = Variable.get("STAGING_EVENTS"),
    s3_bucket = Variable.get("LOG_DATA"),
    execution_date = '{{execution_date.year}}/{{execution_date.month}}/{{ds}}-events.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    dag = dag,
    table = Variable.get("STAGING_SONGS"),
    s3_bucket = Variable.get("SONG_DATA"),
)

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    table = Variable.get("SONGPLAY_TABLE"),
    sql = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = 'Load_user_dim_table',
    dag = dag,
    table = Variable.get("USER_TABLE"),
    sql = SqlQueries.user_table_insert,
    truncate = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    dag = dag,
    table = Variable.get("SONG_TABLE"),
    sql = SqlQueries.song_table_insert,
    truncate = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    dag = dag,
    table = Variable.get("ARTIST_TABLE"),
    sql = SqlQueries.artist_table_insert,
    truncate = True
)

load_time_dimension_table = LoadDimensionOperator(
   task_id = 'Load_time_dim_table',
   dag = dag,
   table = Variable.get("TIME_TABLE"),
   sql = SqlQueries.time_table_insert,
   truncate = True
)

run_quality_checks = DataQualityOperator(
    task_id = 'Run_data_quality_checks',
    dag = dag,
 )

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> (stage_events_to_redshift, stage_songs_to_redshift)  >> load_songplays_table
load_songplays_table >> (load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table) >> run_quality_checks >> end_operator