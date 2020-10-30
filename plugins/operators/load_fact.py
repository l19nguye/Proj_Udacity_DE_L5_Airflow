from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    '''
        LoadFactOperator will be used to create a task which select data 
        from staging tables then insert into fact table.
        
        Will have 3 parameters:
            - ui_color: the color of task will displayed in Airflow Web server.
            - insert_sql: the template of insert SQL command to adding data.            
        
        Define class with following attributes:
            - table: is the fact table name.
            - sql: the SELECT SQL command querying data from staging table.
            - redshift_conn_id: Redshift connection   
    '''

    ui_color = '#F98866'
    
    insert_sql = """
                INSERT INTO {}
                {}
                """

    @apply_defaults
    def __init__(self,
                 table="",
                 sql="",
                 redshift_conn_id="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.sql = sql
        self.redshift_conn_id=redshift_conn_id

    def execute(self, context):
        '''
            - Retrieve and initialize Redshift connection
            - Format INSERT SQL command with table name and SELECT query
            - Redshift run the formatted INSERT SQL command to adding data.           
        '''
        
        self.log.info('LoadFactOperator is executing.')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id) 
        
        formatted_sql = LoadFactOperator.insert_sql.format(
                            self.table,
                            self.sql                            
                        )
        
        redshift.run(formatted_sql)
        
        self.log.info('LoadFactOperator finished.')