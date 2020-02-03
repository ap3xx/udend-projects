from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 
                 redshift_conn_id="",
                 sql_query="",
                 table="",
                 append=False,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.append = append
        
    def execute(self, context):
        self.log.info(f'Loading dimension table: {self.table}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if(self.append==False):
            self.log.info(f'Truncating table before loading...')
            truncate_sql = "DELETE FROM {}".format(self.table)
            redshift.run(truncate_sql)
            
        formatted_sql = "INSERT INTO {} ".format(self.table)+self.sql_query
        redshift.run(formatted_sql)
