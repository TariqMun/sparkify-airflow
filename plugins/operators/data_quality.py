from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for table in self.tables:
            self.log.info(f"Checking data quality for table {table}")
            
            # Check if table has records
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            
            # Check for NULL values in key columns
            null_checks = {
                'users': 'user_id',
                'songs': 'song_id',
                'artists': 'artist_id',
                'time': 'start_time',
                'songplays': 'playid'
            }
            
            if table in null_checks:
                column = null_checks[table]
                null_records = redshift.get_records(
                    f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL")
                
                if null_records[0][0] > 0:
                    raise ValueError(
                        f"Data quality check failed. {table} contains NULL values in {column}")
            
            self.log.info(f"Data quality on table {table} check passed with {num_records} records")
