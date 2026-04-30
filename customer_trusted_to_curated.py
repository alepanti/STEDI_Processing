import hashlib
import sys

from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsgluedq.transforms import EvaluateDataQuality
from awsglueml.transforms import EntityDetector
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import StringType


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1777559737740 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": "false"},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://d609-udacity/accelerometer/"], "recurse": True},
    transformation_ctx="accelerometer_trusted_node1777559737740",
)

# Script generated for node customer_trusted
customer_trusted_node1777559735717 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": "false"},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://d609-udacity/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1777559735717",
)

# Script generated for node SQL Query
SqlQuery0 = """
select DISTINCT c.* from customer_trusted c 
inner join accelerometer_trusted a 
    on a.user = c.email
"""
SQLQuery_node1777560510390 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "accelerometer_trusted": accelerometer_trusted_node1777559737740,
        "customer_trusted": customer_trusted_node1777559735717,
    },
    transformation_ctx="SQLQuery_node1777560510390",
)

# Script generated for node Detect Sensitive Data
entity_detector = EntityDetector()
classified_map = entity_detector.classify_columns(
    SQLQuery_node1777560510390,
    ["EMAIL", "PHONE_NUMBER", "PERSON_NAME"],
    1.0,
    0.1,
    "HIGH",
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


DetectSensitiveData_node1777560638326 = hashDf(
    SQLQuery_node1777560510390, list(classified_map.keys())
)

# Script generated for node customer_curated
EvaluateDataQuality().process_rows(
    frame=DetectSensitiveData_node1777560638326,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1777560014907",
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL",
    },
)
customer_curated_node1777560598742 = glueContext.getSink(
    path="s3://d609-udacity/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_curated_node1777560598742",
)
customer_curated_node1777560598742.setCatalogInfo(
    catalogDatabase="stedi_db", catalogTableName="customer_curated"
)
customer_curated_node1777560598742.setFormat("json")
customer_curated_node1777560598742.writeFrame(DetectSensitiveData_node1777560638326)
job.commit()
