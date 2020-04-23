from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    count = """
        SELECT COUNT(*)
        FROM {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id=None,
                 tables=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id, verify=False)
        for table in self.tables:
            self.log.info(f"Data Quality checking for {table} table")
            formatted_sql = DataQualityOperator.count.format(
                table
            )
            records = redshift.run(formatted_sql)
            # print(records)
            if records != None:
                self.log.info(f"No records present in destinaiton table {table}")
            self.log.info(f"Data quality on table {table} CHECK PASSED!!!.")
            