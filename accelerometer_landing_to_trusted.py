import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node Customer Trusted
CustomerTrusted_node1745597455582 = glueContext.create_dynamic_frame.from_catalog(database="step_trainer_db", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1745597455582")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1745597478791 = glueContext.create_dynamic_frame.from_catalog(database="step_trainer_db", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1745597478791")

# Script generated for node Privacy Filter
PrivacyFilter_node1745597492182 = Join.apply(frame1=CustomerTrusted_node1745597455582, frame2=AccelerometerLanding_node1745597478791, keys1=["email"], keys2=["user"], transformation_ctx="PrivacyFilter_node1745597492182")

# Script generated for node Select Fields
SelectFields_node1745597912585 = SelectFields.apply(frame=PrivacyFilter_node1745597492182, paths=["timestamp", "x", "y", "z", "user"], transformation_ctx="SelectFields_node1745597912585")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SelectFields_node1745597912585, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745597411144", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1745597554648 = glueContext.getSink(path="s3://projects-0a2497c6-0bf0-44e1-b6f6-44c78604c8bc/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1745597554648")
AmazonS3_node1745597554648.setCatalogInfo(catalogDatabase="step_trainer_db",catalogTableName="accelerometer_trusted")
AmazonS3_node1745597554648.setFormat("glueparquet", compression="snappy")
AmazonS3_node1745597554648.writeFrame(SelectFields_node1745597912585)
job.commit()