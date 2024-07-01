from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,       
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup':False,
    'schedule_interval': '@hourly'
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    # schedule_interval='0 * * * *'
    
)
def final_project():

    staging_songs_tbl  = "staging_songs"
    staging_events_tbl = "staging_events"
    fact_songplays_tbl = "songplays"
    dimen_users_tbl      = "users"
    dimen_time_tbl       = "time"
    dimen_artists_tbl    = "artists"
    dimen_songs_tbl      = "songs"

    start_operator = EmptyOperator(task_id='Begin_execution')

    # Defining the S3 Bucket and S3 Key for the Log Data to be loaded in the staging events table
    s3_bucket="udacity-dend"
    s3_key="log_data"    

    # Stage Events to Redshift, extra params are used as a FORMAT JSON in this case
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        redshift_table_name=staging_events_tbl,
        extra_params="format as json 's3://udacity-dend/log_json_path.json'",
    )

    # Defining S3 Key and then Stage Songs to Redshift
    s3_key="song-data"

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        redshift_tbl_name=staging_songs_tbl,
        extra_params="REGION 'us-west-2'  FORMAT as JSON 'auto' TRUNCATECOLUMNS BLANKSASNULL  EMPTYASNULL"  
    )
    # Load Song Facts Table
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id = "redshift",
        aws_credentials_id="aws_credentials",
        tbl_name=fact_songplays_tbl,
        sql_statement=SqlQueries.songplay_table_insert,
        append_data=False
        
    )
    # Load User Dimension Table
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id = "redshift",
        aws_credentials_id="aws_credentials",
        table_name = dimen_users_tbl,
        sql_statement = SqlQueries.user_table_insert,
        append_data=False
    )
    # Load Song Dimension Table
    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id = "redshift",
        aws_credentials_id="aws_credentials",
        table_name = dimen_songs_tbl,
        sql_statement = SqlQueries.song_table_insert,
        append_data=False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id = "redshift",
        aws_credentials_id="aws_credentials",
        table_name = dimen_artists_tbl,
        sql_statement = SqlQueries.artist_table_insert,
        append_data=False

    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id = "redshift",
        aws_credentials_id="aws_credentials",
        table_name = dimen_time_tbl,
        sql_statement = SqlQueries.time_table_insert,
        append_data=False
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id = "redshift",
        aws_credentials_id ="aws_credentials",
        input_staging_songs_table = staging_songs_tbl,
        input_staging_events_table = staging_events_tbl,
        input_fact_songplays_table = fact_songplays_tbl,
        input_dimension_users_table = dimen_users_tbl,
        input_dimension_time_table  = dimen_time_tbl,
        input_dimension_artists_table = dimen_artists_tbl,
        input_dimension_songs_table  = dimen_songs_tbl,
        data_quality_checks=
            [       
            {'dq_check_sql': 'select count(*) from songs where title is null', 'expected_value': 0},
            {'dq_check_sql': 'select count(*) from artists where name is null', 'expected_value': 0 },
            {'dq_check_sql': 'select count(*) from users where first_name is null', 'expected_value': 0},
            {'dq_check_sql': 'select count(*) from time where month is null', 'expected_value': 0},
            {'dq_check_sql': 'select count(*) from songsplay where userid is null', 'expected_value': 0 }
            ]
    )
    
    end_operator = EmptyOperator(task_id='end_execution')
    # DAG Task Dependency(Data Lineage as per the Data Requirements)

    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift

    stage_events_to_redshift >> load_songplays_table 
    stage_songs_to_redshift >> load_songplays_table

    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table

    load_user_dimension_table >> run_quality_checks
    load_song_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks

    run_quality_checks >> end_operator

final_project_dag = final_project()
