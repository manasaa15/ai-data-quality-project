import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource = glueContext.create_dynamic_frame.from_catalog(
    database="ai_project_db",
    table_name="creditcard"
)

df = datasource.toDF()

df = df.dropDuplicates()
df = df.fillna(0)

df = df.withColumn("amount_log", col("Amount") + 1)

cleaned_dynamic = DynamicFrame.fromDF(df, glueContext, "cleaned_dynamic")

glueContext.write_dynamic_frame.from_options(
    frame=cleaned_dynamic,
    connection_type="s3",
    connection_options={
        "path": "s3://ai-data-quality-project-manasa/cleaned/"
    },
    format="parquet"
)

job.commit()
