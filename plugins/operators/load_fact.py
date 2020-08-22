# -*- coding: utf-8 -*-
"""
Created on Sat Aug 22 2020
@author: gari.ciodaro.guerra
Custom Postgress operator to load data into fact table
tables.
"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'
    @apply_defaults
    def __init__(self,
                 table,
                 insert_statement,
                 redshift_conn_id,
                 operation,
                 *args, **kwargs):
        """ Operator constructor

        Parameters
        ----------
        table : string
        insert_statement : string
        redshift_conn_id : string
        operation : string
            if equalts to truncate, perform trucate
            to dimension.
        """

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table=table
        self.operation =operation
        self.insert_statement=insert_statement
        self.redshift_conn_id=redshift_conn_id

    def execute(self, context):
        self.log.info('LoadFactOperator for table {} '.format(self.table))
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.operation == 'truncate':
            redshift_hook.run('TRUNCATE TABLE {}'.format(self.table))
            
        redshift_hook.run(self.insert_statement)