from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_connection_id = "",
                 aws_credentials_id="",                       
                 input_staging_songs_tbl = "",
                 input_staging_events_tbl = "",
                 input_fact_songplays_tbl = "",
                 input_dim_users_tbl = "",
                 input_dim_time_tbl = "",
                 input_dim_artists_tbl = "",
                 input_dim_songs_tbl = "",
                 data_quality_checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_connection_id = redshift_connection_id
        self.aws_credentials_id = aws_credentials_id
        self.input_staging_songs_tbl = input_staging_songs_tbl
        self.input_staging_events_tbl = input_staging_events_tbl
        self.input_fact_songplays_tbl = input_fact_songplays_tbl
        self.input_dim_users_tbl = input_dim_users_tbl
        self.input_dim_time_tbl = input_dim_time_tbl
        self.input_dim_artists_tbl = input_dim_artists_tbl
        self.input_dim_songs_tbl = input_dim_songs_tbl
        self.data_quality_checks = data_quality_checks

    def execute(self, context):
        self.log.info('Starting DataQualityOperator')


        redshift_hook    =   PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Running the Data Quality Checks')
        input_staging_songs_tbl_records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.input_staging_songs_tbl}")
        input_staging_events_tbl_records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.input_staging_events_tbl}")
        input_fact_songplays_tbl_records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.input_fact_songplays_tbl}")
        input_dim_users_tbl_records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.input_dim_users_tbl}")
        input_dim_time_tbl_records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.input_dim_time_tbl}")
        input_dim_artists_tbl_records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.input_dim_artists_tbl}")
        input_dim_songs_tbl_records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.input_dim_songs_tbl}")
  
        self.log.info(f"Data quality checks on table {self.input_staging_songs_tbl} ---> {input_staging_songs_tbl_records} records") 
        self.log.info(f"Data quality checks on table {self.input_staging_events_table} ---> {input_staging_events_tbl_records} records") 
        self.log.info(f"Data quality checks on table {self.input_fact_songplays_table} ---> {input_fact_songplays_tbl_records} records") 
        self.log.info(f"Data quality checks on table {self.input_dim_users_table} ---> {input_dim_users_tbl_records} records")
        self.log.info(f"Data quality checks on table {self.input_dim_time_table} ---> {input_dim_time_tbl_records} records")
        self.log.info(f"Data quality checks on table {self.input_dim_artists_table} ---> {input_dim_artists_tbl_records} records")
        self.log.info(f"Data quality checks on table {self.input_dim_songs_table} ---> {input_dim_songs_tbl_records} records")

        # Has Same Rows/Results Check
        if len(input_staging_songs_tbl_records) < 1 :
                raise ValueError(f"Data quality check failed. {self.input_staging_songs_tbl} returned no results")
        
        if len(input_staging_events_tbl_records) < 1 :
                raise ValueError(f"Data quality check failed. {self.input_staging_events_tbl} returned no results")
        
        if len(input_fact_songplays_tbl_records) < 1 :
                raise ValueError(f"Data quality check failed. {self.input_fact_songplays_tbl} returned no results")
        
        if len(input_dim_users_tbl_records) < 1 :
                raise ValueError(f"Data quality check failed. {self.input_dim_users_tbl} returned no results")
        
        if len(input_dim_time_tbl_records) < 1 :
                raise ValueError(f"Data quality check failed. {self.input_dim_time_tbl} returned no results")
        
        if len(input_dim_songs_tbl_records) < 1 :
                raise ValueError(f"Data quality check failed. {self.input_dim_songs_tbl} returned no results")
        
        if len(input_dim_artists_tbl_records) < 1 :
                raise ValueError(f"Data quality check failed. {self.input_dim_artists_tbl} returned no results")
        
        error_count  = 0
        
        for check_step in self.data_quality_checks:
            
            dq_check_query = check_step.get('dq_check_sql')
            expected_result = check_step.get('expected_value')
            
            result = redshift_hook.get_records(dq_check_query)[0]
            
            self.log.info(f"Running Data Quality query   : {dq_check_query}")
            self.log.info(f"Expected Data Quality result : {expected_result}")
            self.log.info(f"Compare result    : {result}")
            
            
            if result[0] != expected_result:
                error_count += 1
                self.log.info(f"Data quality check fails At   : {dq_check_query}")
                
            
        if error_count > 0:
            self.log.info('DQ checks - Failed')
        else:
            self.log.info('DQ checks - Passed')