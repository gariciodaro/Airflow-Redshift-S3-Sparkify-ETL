# -*- coding: utf-8 -*-
"""
Created on Sat Aug 22 2020
@author: gari.ciodaro.guerra
Custom Postgress operator to perform data quality check.
"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'
    @apply_defaults
    def __init__(self,
                 table_list,
                 redshift_conn_id,
                 *args, **kwargs):
        """ Operator constructor

        Parameters
        ----------
        table_list : list
        redshift_conn_id : string
            conection id of redshift. Configured on Airflow web server.
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table_list = table_list
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        for table in self.table_list:
            self.log.info(f'Begin data quality on table: {table} ')

            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Quality check failed. No records for {table}")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Quality check failed. {table} had 0 rows")
            logging.info(
                f"Quality check passed. {table} has with {records[0][0]} records")