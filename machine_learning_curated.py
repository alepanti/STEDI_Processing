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
accelerometer_trusted_node1777588658305 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1777588658305",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1777588659873 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_node1777588659873",
)

# Script generated for node SQL Query
SqlQuery0 = """
SELECT * FROM step_trainer_trusted st
INNER JOIN accelerometer_trusted a
    ON a.timestamp = st.sensorreadingtime
"""
SQLQuery_node1777518121066 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "accelerometer_trusted": accelerometer_trusted_node1777588658305,
        "step_trainer_trusted": step_trainer_trusted_node1777588659873,
    },
    transformation_ctx="SQLQuery_node1777518121066",
)

# Script generated for node Detect Sensitive Data
entity_detector = EntityDetector()
classified_map = entity_detector.classify_columns(
    SQLQuery_node1777518121066, ["EMAIL"], 1.0, 0.1, "HIGH"
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


DetectSensitiveData_node1777519959316 = hashDf(
    SQLQuery_node1777518121066, list(classified_map.keys())
)

# Script generated for node machine_learning_curated
EvaluateDataQuality().process_rows(
    frame=DetectSensitiveData_node1777519959316,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1777518868795",
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL",
    },
)
machine_learning_curated_node1777519145937 = glueContext.getSink(
    path="s3://d609-udacity/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_curated_node1777519145937",
)
machine_learning_curated_node1777519145937.setCatalogInfo(
    catalogDatabase="stedi_db", catalogTableName="machine_learning_curated"
)
machine_learning_curated_node1777519145937.setFormat("json")
machine_learning_curated_node1777519145937.writeFrame(
    DetectSensitiveData_node1777519959316
)
job.commit()
