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

# Script generated for node customer_landing
customer_landing_node1777559555373 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": "false"},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://d609-udacity/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="customer_landing_node1777559555373",
)

# Script generated for node SQL Query
SqlQuery0 = """
select * from customer_landing
where sharewithresearchasofdate IS NOT NULL
"""
SQLQuery_node1777559576428 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"customer_landing": customer_landing_node1777559555373},
    transformation_ctx="SQLQuery_node1777559576428",
)

# Script generated for node change to timestamp
changetotimestamp_node1777560024856 = ApplyMapping.apply(
    frame=SQLQuery_node1777559576428,
    mappings=[
        ("customerName", "string", "customerName", "string"),
        ("email", "string", "email", "string"),
        ("phone", "string", "phone", "string"),
        ("birthDay", "string", "birthDay", "date"),
        ("serialNumber", "string", "serialNumber", "string"),
        ("registrationDate", "bigint", "registrationDate", "timestamp"),
        ("lastUpdateDate", "bigint", "lastUpdateDate", "timestamp"),
        (
            "shareWithResearchAsOfDate",
            "bigint",
            "shareWithResearchAsOfDate",
            "timestamp",
        ),
        ("shareWithPublicAsOfDate", "bigint", "shareWithPublicAsOfDate", "timestamp"),
        ("shareWithFriendsAsOfDate", "bigint", "shareWithFriendsAsOfDate", "timestamp"),
    ],
    transformation_ctx="changetotimestamp_node1777560024856",
)

# Script generated for node customer_trusted
EvaluateDataQuality().process_rows(
    frame=changetotimestamp_node1777560024856,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1777559511709",
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL",
    },
)
customer_trusted_node1777559637708 = glueContext.getSink(
    path="s3://d609-udacity/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_trusted_node1777559637708",
)
customer_trusted_node1777559637708.setCatalogInfo(
    catalogDatabase="stedi_db", catalogTableName="customer_trusted"
)
customer_trusted_node1777559637708.setFormat("json")
customer_trusted_node1777559637708.writeFrame(changetotimestamp_node1777560024856)
job.commit()
