# -*- coding: utf-8 -*-
"""
Created on Sat Aug 22 2020
@author: gari.ciodaro.guerra
Custom Postgress operator to load data from
S3 to Redshift staging area.
tables.
"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    truncate_sql="TRUNCATE TABLE {};"
    copy_sql = ("""
    COPY {} FROM '{}'
    format as json '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    region 'us-west-2';
    """)

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id,
                 s3_path,
                 credentials,
                 s3_json_option='auto',
                 *args, **kwargs):
        """ Operator constructor
        Parameters
        ----------
        table : string
        redshift_conn_id : string
        s3_path : string
        access_key : string
        secret_key : string
        s3_json_option : string
            set auto if path contains json structure, else
            pass structure file.
        """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table=table
        self.redshift_conn_id = redshift_conn_id
        self.aws_access_key=credentials.access_key
        self.aws_secret_key=credentials.secret_key
        self.s3_path = s3_path
        self.s3_json_option = s3_json_option

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        
        self.log.info('StageToRedshiftOperator. truncating {} '.format(self.table))
        redshift_hook.run(StageToRedshiftOperator.truncate_sql.format(self.table))
        
        self.log.info('StageToRedshiftOperator. Staging {} '.format(self.table))
        execute_q=StageToRedshiftOperator.copy_sql.format(self.table,
                                                          self.s3_path,
                                                          self.s3_json_option,
                                                          self.aws_access_key,
                                                          self.aws_secret_key)
        self.log.info(execute_q)
        redshift_hook.run(execute_q)