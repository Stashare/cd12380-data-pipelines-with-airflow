from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks import AwsHook
# from airflow.contrib.hooks.aws_hook import AwsHook 
from airflow.secrets.metastore import MetastoreBackend


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 redshift_tbl_name="",
                 s3_bucket="udacity-dend",
                 s3_key="",
                 extra_params="",
                 *args,
                 **kwargs
                ):
        

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.redshift_tbl_name = redshift_tbl_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.extra_params = extra_params

    def execute(self, context):
        self.log.info('Starting StageToRedshiftOperator')
        aws_hook    =   AwsHook(self.aws_creds_id)
        self.log.info('Gettting AWS Credentials from the Airflow WebUI AWS Hook')

        credentials =   aws_hook.get_credentials()
        # metastoreBackend = MetastoreBackend()
        # credentials = metastoreBackend.get_connection("aws_credentials")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Deleting data from target staging Redshift table before fresh data load")
        redshift.run("TRUNCATE {}".format(self.redshift_table_name))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info(f'S3 path: {s3_path}')
        
        self.log.info('Formatting SQL for StageToRedshiftOperator')
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.redshift_table_name,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.extra_params

        )

        self.log.info(formatted_sql)
        redshift.run(formatted_sql)
        self.log.info('Formatted SQL running completed')






