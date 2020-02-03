from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 tableList="",
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.tableList = tableList

    def execute(self, context):
        self.log.info('Starting Data Quality check...')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        qualityFlag = True
        
        for table in self.tableList:
            self.log.info(f'Checking table: {table}')
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                qualityFlag= False
                self.log.warning(f'Table {table} not alright!')
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                qualityFlag= False
                self.log.warning(f'Table {table} not alright!')
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
            
        if (qualityFlag == True):
            self.log.info(f"Data Quality Test PASSED for all tables!")
        else:
            self.log.info(f"Data Quality Test FAILED! Check any table to see more details...")
