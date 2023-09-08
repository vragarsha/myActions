import clickhouse_driver
import boto3
import pandas as pd
import datetime
from io import StringIO

# ClickHouse connection settings
# Python Driver: https://clickhouse-driver.readthedocs.io/en/latest/index.html
clickhouse_host = 'clickhouse_host'
# Native (TCP) protocol (port 9000 by default)
clickhouse_port = 9000
clickhouse_user = 'clickhouse_user'
clickhouse_password = 'clickhouse_password'
clickhouse_db = 'clickhouse_db'

# AWS S3 settings
# Customer-provided AWS S3 bucket credentials
aws_access_key_id = 'aws_access_key_id'
aws_secret_access_key = 'aws_secret_access_key'
s3_bucket = 's3_bucket'

# Calculate the date for yesterday
yesterday = datetime.date.today() - datetime.timedelta(days=1)

# Initialize ClickHouse connection
connection = clickhouse_driver.connect(
    host=clickhouse_host,
    port=clickhouse_port,
    user=clickhouse_user,
    password=clickhouse_password,
    database=clickhouse_db,
)

# Query ClickHouse to get average and 90th percentile call length for each agent
# Using quantile function instead of percentile_cont to calculate the 90th percentile as per documentation: https://clickhouse.com/docs/en/sql-reference/aggregate-functions/reference/quantile
query = f"""
    SELECT
        agent_id,
        AVG(call_duration_sec) AS avg_call_length,
        quantile(0.9)(call_duration_sec) AS percentile_90_call_length
    FROM
        conversations
    WHERE
        toDate(call_start) = '{yesterday}'
    GROUP BY
        agent_id
"""

# Execute the query
cursor = connection.cursor()
cursor.execute(query)

# Fetch the results into a Pandas DataFrame
results = cursor.fetchall()
df = pd.DataFrame(results, columns=['agent_id', 'avg_call_length', 'percentile_90_call_length'])

# Convert the DataFrame to CSV format
csv_data = df.to_csv(index=False)

# Initialize AWS S3 client
# Boto3: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)

# Define the S3 object key (file name) with yesterday's date
s3_object_key = f'call_length_{yesterday}.csv'

# Upload the CSV data to the specified S3 bucket
s3.put_object(Bucket=s3_bucket, Key=s3_object_key, Body=csv_data)

# Close ClickHouse connection
connection.close()
