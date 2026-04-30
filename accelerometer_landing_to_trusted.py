import sys

from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.context import SparkContext


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

# Script generated for node accelerometer_landing
accelerometer_landing_node1777558502298 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": "false"},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://d609-udacity/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1777558502298",
)

# Script generated for node customer_trusted
customer_trusted_node1777560250217 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": "false"},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://d609-udacity/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1777560250217",
)

# Script generated for node update timestamp type
updatetimestamptype_node1777505905483 = ApplyMapping.apply(
    frame=accelerometer_landing_node1777558502298,
    mappings=[
        ("user", "string", "user", "string"),
        ("timestamp", "bigint", "timestamp", "timestamp"),
        ("x", "double", "x", "double"),
        ("y", "double", "y", "double"),
        ("z", "double", "z", "double"),
    ],
    transformation_ctx="updatetimestamptype_node1777505905483",
)

# Script generated for node SQL Query
SqlQuery0 = """
select al.* from accelerometer_landing al
inner join customer_trusted ct
    on ct.email = al.user
WHERE al.timestamp >= ct.sharewithresearchasofdate
"""
SQLQuery_node1777505130955 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "accelerometer_landing": updatetimestamptype_node1777505905483,
        "customer_trusted": customer_trusted_node1777560250217,
    },
    transformation_ctx="SQLQuery_node1777505130955",
)

# Script generated for node accelerometer_trusted
EvaluateDataQuality().process_rows(
    frame=SQLQuery_node1777505130955,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1777500675439",
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL",
    },
)
accelerometer_trusted_node1777505301419 = glueContext.getSink(
    path="s3://d609-udacity/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trusted_node1777505301419",
)
accelerometer_trusted_node1777505301419.setCatalogInfo(
    catalogDatabase="stedi_db", catalogTableName="accelerometer_trusted"
)
accelerometer_trusted_node1777505301419.setFormat("json")
accelerometer_trusted_node1777505301419.writeFrame(SQLQuery_node1777505130955)
job.commit()
