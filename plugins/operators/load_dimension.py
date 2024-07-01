from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_connection_id = "",
                 aws_credentials_id = "",
                 tbl_name="",
                 sql_statement="",
                 append_data=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_connection_id = redshift_connection_id
        self.aws_credentials_id = aws_credentials_id
        self.tbl_name = tbl_name
        self.sql_statement = sql_statement
        self.append_data = append_data


    def execute(self, context):
        self.log.info('Starting LoadDimensionOperator')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_connection_id)

        self.log.info(f'loading data to {self.table_name} dimension table.')

        if self.append_data == False:            
            redshift_hook.run(f"TRUNCATE {self.table_name}")
        redshift_hook.run(f"""INSERT INTO {self.table_name} 
                              {self.sql_statement} ;""")
