# Sparkify Data Pipelines with Airflow

This project creates automated data pipelines for Sparkify's music streaming data using Apache Airflow. The pipeline extracts data from S3, stages it in Redshift, and transforms it into a star schema optimized for song play analysis.

## Project Structure

- `dags/sparkify_etl_dag.py`: Main Airflow DAG definition
- `plugins/helpers/sql_queries.py`: SQL queries for table creation and ETL
- `plugins/operators/`: Custom Airflow operators
  - `stage_redshift.py`: Stages data from S3 to Redshift
  - `load_fact.py`: Loads fact tables
  - `load_dimension.py`: Loads dimension tables
  - `data_quality.py`: Runs data quality checks

## Setup Instructions

1. **Prerequisites**:
   - AWS account with Redshift cluster
   - Airflow environment (or use Udacity workspace)
   - Data copied to S3 bucket (`s3://sparki20250418/`)

2. **Airflow Connections**:
   - Set up AWS credentials connection in Airflow (conn_id: `aws_credentials`)
   - Set up Redshift connection in Airflow (conn_id: `redshift`)

3. **Install dependencies**:
  - Python 3.6+ (Airflow 1.10.x supports Python 3.6-3.8)
   - PostgreSQL client libraries (for psycopg2)
   - AWS CLI configured with proper credentials
   ```bash
   pip install -r requirements.txt

The DAG performs the following steps:

1. **Stage data**:
- Loads song data from `s3://sparki20250418/song-data/`
- Loads log data from `s3://sparki20250418/log-data/`

2. **Transform data**:
- Creates fact table (songplays)
- Creates dimension tables (users, songs, artists, time)

3. **Data quality checks**:
- Verifies tables contain data
- Checks for NULL values in key columns

## Custom Operators

- **StageToRedshiftOperator**: Copies JSON data from S3 to Redshift
- **LoadFactOperator**: Loads data into fact tables
- **LoadDimensionOperator**: Loads data into dimension tables (with truncate option)
- **DataQualityOperator**: Runs data quality checks on tables

## Project Notes

Due to local environment constraints preventing Airflow execution, this repository demonstrates:
- Proper DAG structure and task dependencies
- Implementation of all required custom operators
- Configuration for your specific S3 bucket paths
- Complete SQL transformations for the ETL process
- Data quality checking implementation
