import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1745601638397 = glueContext.create_dynamic_frame.from_catalog(database="step_trainer_db", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1745601638397")

# Script generated for node Customer Trusted
CustomerTrusted_node1745601622940 = glueContext.create_dynamic_frame.from_catalog(database="step_trainer_db", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1745601622940")

# Script generated for node Join
Join_node1745601740244 = Join.apply(frame1=CustomerTrusted_node1745601622940, frame2=AccelerometerTrusted_node1745601638397, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1745601740244")

# Script generated for node Drop Duplicates
DropDuplicates_node1745601760394 =  DynamicFrame.fromDF(Join_node1745601740244.toDF().dropDuplicates(["customername"]), glueContext, "DropDuplicates_node1745601760394")

# Script generated for node Customers Curated
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1745601760394, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745600057941", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomersCurated_node1745601804295 = glueContext.getSink(path="s3://projects-0a2497c6-0bf0-44e1-b6f6-44c78604c8bc/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomersCurated_node1745601804295")
CustomersCurated_node1745601804295.setCatalogInfo(catalogDatabase="step_trainer_db",catalogTableName="customers_curated")
CustomersCurated_node1745601804295.setFormat("glueparquet", compression="snappy")
CustomersCurated_node1745601804295.writeFrame(DropDuplicates_node1745601760394)
job.commit()