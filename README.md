# AirFLow DAG for Sparkify ETL.
Project: Data Pipelines with Airflow

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

```
├── dag.png
├── dags
│   ├── create_tables_dag.py
│   └── etl_dag.py
├── plugins
│   ├── helpers
│   │   ├── __init__.py
│   │   └── sql_queries.py
│   ├── __init__.py
│   └── operators
│       ├── data_quality.py
│       ├── __init__.py
│       ├── load_dimension.py
│       ├── load_fact.py
│       └── stage_redshift.py
└── README.md

```

<div>
<img src="./dag.png">
</div>

## Udacity project.
still under development.
