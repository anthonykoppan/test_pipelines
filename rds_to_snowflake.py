import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# --- Job setup ---
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --- Step 1: Read data from RDS (example: PostgreSQL RDS) ---
# Replace "my-rds-conn" with your Glue JDBC connection name
rds_df = glueContext.create_dynamic_frame.from_options(
    connection_type="postgresql",   # or "mysql" or "sqlserver"
    connection_options={
        "connectionName": "my-rds-conn",
        "dbtable": "public.customers"   # your RDS table
    }
)

# --- Step 2: Optional transforms ---
mapped_df = rds_df.apply_mapping([
    ("customer_id", "int", "customer_id", "int"),
    ("first_name", "string", "first_name", "string"),
    ("last_name", "string", "last_name", "string"),
    ("email", "string", "email", "string"),
    ("signup_date", "timestamp", "signup_date", "timestamp"),
    ("is_active", "boolean", "is_active", "boolean"),
    ("lifetime_value", "decimal(18,2)", "lifetime_value", "decimal(18,2)")
])

# --- Step 3: Write to Snowflake ---
snowflake_options = {
    "sfURL": "myaccount.snowflakecomputing.com",   # your Snowflake URL
    "sfDatabase": "MY_DB",
    "sfSchema": "RAW_STAGE",
    "sfWarehouse": "MY_WH",
    "sfRole": "SYSADMIN",
    "sfUser": "MYUSER",
    "sfPassword": "MYPASSWORD",   # consider Secrets Manager
    "dbtable": "CUSTOMERS"
}

glueContext.write_dynamic_frame.from_options(
    frame=mapped_df,
    connection_type="snowflake",
    connection_options=snowflake_options
)

job.commit()