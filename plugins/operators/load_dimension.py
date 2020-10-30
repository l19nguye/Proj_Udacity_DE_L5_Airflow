from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    '''
        LoadDimensionOperator will be used to create a task which select data 
        from staginng tables then insert into dimension table.
        
        Will have 3 parameters:
            - ui_color: the color of task will displayed in Airflow Web server.
            - truncate_sql: the template of truncate SQL command to clear existing data.
            - insert_sql: the template of insert SQL command to adding data.
            
        
        Define class with following attributes:
            - table: is the dimension table name.
            - sql: the SELECT SQL command querying data from fact table.
            - redshift_conn_id: Redshift connection
            - truncate: flag to identify whether need to truncate table before inserting data into table.    
    '''

    ui_color = '#80BD9E'
    
    truncate_sql = " TRUNCATE {};"
    
    insert_sql = """
                INSERT INTO {}
                {};
                """

    @apply_defaults
    def __init__(self,
                 table="",
                 sql="",
                 redshift_conn_id="",
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.sql = sql
        self.truncate = truncate
        self.redshift_conn_id=redshift_conn_id
        
    def execute(self, context):
        '''
            - Retrieve and initialize Redshift connection
            - Format INSERT SQL command with table name and SELECT query
            - If truncate flag is True, will need to add TRUNCATE SQL in front of the INSERT SQL
            - Redshift run the formatted SQL command to adding data into dimension table.      
        '''
        
        self.log.info('LoadDimensionOperator is executing dimension table: {}'.format(self.table))
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        formatted_sql = LoadDimensionOperator.insert_sql.format(
                            self.table,
                            self.sql                            
                        )
        
        if self.truncate == True:
            formatted_sql = LoadDimensionOperator.truncate_sql.format(self.table) + formatted_sql
        
        redshift.run(formatted_sql)
        
        self.log.info('LoadDimensionOperator finish dimension table: {}'.format(self.table))