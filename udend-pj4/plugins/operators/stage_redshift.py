from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    
    ui_color = '#358140'
    template_fields    = ("s3_key",)
    sql_csv = """ 
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    REGION '{}'
    CSV COMPUPDATE OFF;
    """
    sql_json = """ 
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    REGION '{}'
    json '{}' COMPUPDATE OFF;
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 s3_region="us-west-2",
                 format="json",
                 jsonPath="auto",
                 ignore_headers=1,
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_region = s3_region
        self.format = format
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.jsonPath = jsonPath

    def execute(self, context):
        self.log.info(f'Loading table {self.table} from s3 into redshift')
        
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if(self.format == 'json'):
            s3_jsonpath = self.jsonPath
            if(self.jsonPath != "auto"):
                s3_jsonpath = "s3://{}/{}".format(self.s3_bucket, self.jsonPath)
            formatted_sql = StageToRedshiftOperator.sql_json.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.s3_region,
                s3_jsonpath)
            
        else:
            formatted_sql = StageToRedshiftOperator.sql_csv.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.s3_region)
       
        redshift.run(formatted_sql)
