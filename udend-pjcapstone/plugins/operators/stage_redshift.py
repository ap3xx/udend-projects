from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable


class StageToRedshiftOperator(BaseOperator):
    
    ui_color = '#358140'
    
    sql_csv = """ 
        COPY {}
        FROM '{}'
        IAM_ROLE '{}'
        REGION '{}'
        CSV COMPUPDATE OFF
    """

    sql_json = """ 
    COPY {}
    FROM '{}'
    IAM_ROLE '{}'
    REGION '{}'
    json '{}';
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 redshift_arn_variable_name="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 s3_region="us-east-1",
                 format="json",
                 jsonPath="auto",
                 ignore_headers=True,
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_region = s3_region
        self.format = format
        self.ignore_headers = ignore_headers
        self.redshift_arn_variable_name = redshift_arn_variable_name
        self.jsonPath = jsonPath

    def execute(self, context):
        self.log.info(f'Loading table {self.table} from s3 into redshift')
        
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.format == 'json' :
            s3_jsonpath = self.jsonPath
            if(self.jsonPath != "auto"):
                s3_jsonpath = "s3://{}/{}".format(self.s3_bucket, self.jsonPath)

            formatted_sql = self.sql_json.format(
                self.table,
                s3_path,
                Variable.get(self.redshift_arn_variable_name),
                self.s3_region,
                s3_jsonpath)
        elif self.format == 'csv':
            query = self.sql_csv
            if self.ignore_headers:
                query += ' IGNOREHEADER 1'

            formatted_sql = query.format(
                self.table,
                s3_path,
                Variable.get(self.redshift_arn_variable_name),
                self.s3_region)
        else:
            raise Exception(f'Format {self.format} not supported!')
       
        redshift.run(formatted_sql)
