import hashlib
import sys

from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsgluedq.transforms import EvaluateDataQuality
from awsglueml.transforms import EntityDetector
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# source accelerometer_landing
accelerometer_trusted = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://d609-udacity/accelerometer/trusted/"]},
    format="json",
)

# source customer_trusted
customer_trusted = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://d609-udacity/customer/trusted/"]},
    format="json",
)

# Register as temp views for Spark SQL
accelerometer_trusted.toDF().createOrReplaceTempView("accelerometer_trusted")
customer_trusted.toDF().createOrReplaceTempView("customer_trusted")

# Use DISTINCT to avoid row multiplication from one-to-many join
curated_df = spark.sql("""
    SELECT DISTINCT c.*
    FROM customer_trusted c
    INNER JOIN accelerometer_trusted a
        ON c.email = a.user
""")

# Convert back to DynamicFrame
curated_dynamic = DynamicFrame.fromDF(curated_df, glueContext, "customer_curated")

entity_detector = EntityDetector()
classified_map = entity_detector.classify_columns(
    curated_dynamic, ["EMAIL", "PERSON_NAME", "PHONE_NUMBER"], 1.0, 0.1, "HIGH"
)


def pii_column_hash(original_cell_value):
    return hashlib.sha256(str(original_cell_value).encode()).hexdigest()


pii_column_hash_udf = udf(pii_column_hash, StringType())


def hashDf(df, keys):
    if not keys:
        return df
    df_to_hash = df.toDF()
    for key in keys:
        df_to_hash = df_to_hash.withColumn(key, pii_column_hash_udf(key))
    return DynamicFrame.fromDF(df_to_hash, glueContext, "updated_hashed_df")


DetectSensitiveData = hashDf(curated_dynamic, list(classified_map.keys()))

EvaluateDataQuality().process_rows(
    frame=DetectSensitiveData,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_at_dynamic",
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL",
    },
)

# Script generated for node Customer Curated
sink = glueContext.getSink(
    path="s3://d609-udacity/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
)
sink.setCatalogInfo(catalogDatabase="stedi_db", catalogTableName="customer_curated")
sink.setFormat("json")
sink.writeFrame(DetectSensitiveData)

job.commit()
