import sys

from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.context import SparkContext
from pyspark.sql.functions import col, from_unixtime, to_timestamp


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
customer_landing_node1777404882965 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": "false"},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://d609-udacity/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="customer_landing_node1777404882965",
)

# Script generated for node filter_shared
SqlQuery155 = """
select * from customer_landing
where sharewithresearchasofdate is not null
"""
filter_shared_node1777404988972 = sparkSqlQuery(
    glueContext,
    query=SqlQuery155,
    mapping={"customer_landing": customer_landing_node1777404882965},
    transformation_ctx="filter_shared_node1777404988972",
)

# Convert date bigint to datetime
df = filter_shared_node1777404988972.toDF()

cols = [
    "registrationdate",
    "lastupdatedate",
    "sharewithresearchasofdate",
    "sharewithpublicasofdate",
    "sharewithfriendsasofdate",
]

for c in cols:
    df = df.withColumn(c, to_timestamp(from_unixtime(col(c) / 1000)))

converted_time_df = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

# Script generated for node customer_trusted
EvaluateDataQuality().process_rows(
    frame=converted_time_df,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1777405602316",
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL",
    },
)
customer_trusted_node1777405617789 = glueContext.getSink(
    path="s3://d609-udacity/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_trusted_node1777405617789",
)
customer_trusted_node1777405617789.setCatalogInfo(
    catalogDatabase="stedi_db", catalogTableName="customer_trusted"
)
customer_trusted_node1777405617789.setFormat("json")
customer_trusted_node1777405617789.writeFrame(converted_time_df)
job.commit()
