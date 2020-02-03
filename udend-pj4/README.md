# udend-pj4

Fourth project for Udacity Data Engineering Nanodegree. 

## Introduction

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

## Project Description

This is an ETL Pipeline created using Apache Airflow that consists of developing an ETL job to read the files from an S3 bucket and load to an AWS Redshift Cluster in Staging Tables.

Subsequently the data is loaded on to the analysis tables from the staging tables. A Data Quality Check is performed to verify if the data is properly loaded on the Analysis tables.

## Dags list

![dags_list](./docs/dags_list.png)

## Main DAG graph view

![sparkify_etl](./docs/sparkify_etl.png)

## Connections configuration

Access the following menu to manage connections:

![connections](./docs/connections.png)

### AWS Connection

![aws_credentials](./docs/aws_credentials.png)

### Redshift Connection

![redshift_connection](./docs/redshift_connection.png)

## Extras

I've created an additional DAG meant to create the tables at Redshift, or recreated (droped then created again) for the following runs.

![reset_dw](./docs/reset_dw.png)

## Author

* [**Flavio Teixeira**](http://github.com/ap3xx)