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

# Script generated for node customer_curated
customer_curated_node1777561605781 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": "false"},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://d609-udacity/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="customer_curated_node1777561605781",
)

# Script generated for node step_trainer_landing
step_trainer_landing_node1777558599139 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": "false"},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://d609-udacity/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_node1777558599139",
)

# Script generated for node SQL Query
SqlQuery0 = """
select s.* FROM step_trainer_landing s 
inner join customer_curated c 
    on c.serialnumber = s.serialnumber
"""
SQLQuery_node1777517477095 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "step_trainer_landing": step_trainer_landing_node1777558599139,
        "customer_curated": customer_curated_node1777561605781,
    },
    transformation_ctx="SQLQuery_node1777517477095",
)

# Script generated for node Change Schema
ChangeSchema_node1777563372959 = ApplyMapping.apply(
    frame=SQLQuery_node1777517477095,
    mappings=[
        ("sensorReadingTime", "long", "sensorReadingTime", "timestamp"),
        ("serialNumber", "string", "serialNumber", "string"),
        ("distanceFromObject", "int", "distanceFromObject", "int"),
    ],
    transformation_ctx="ChangeSchema_node1777563372959",
)

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(
    frame=ChangeSchema_node1777563372959,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1777517302403",
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL",
    },
)
step_trainer_trusted_node1777517660162 = glueContext.getSink(
    path="s3://d609-udacity/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_node1777517660162",
)
step_trainer_trusted_node1777517660162.setCatalogInfo(
    catalogDatabase="stedi_db", catalogTableName="step_trainer_trusted"
)
step_trainer_trusted_node1777517660162.setFormat("json")
step_trainer_trusted_node1777517660162.writeFrame(ChangeSchema_node1777563372959)
job.commit()
