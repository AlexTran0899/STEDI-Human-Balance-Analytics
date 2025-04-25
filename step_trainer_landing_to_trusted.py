import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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
CustomerTrusted_node1745597669155 = glueContext.create_dynamic_frame.from_catalog(database="step_trainer_db", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1745597669155")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1745597686931 = glueContext.create_dynamic_frame.from_catalog(database="step_trainer_db", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1745597686931")

# Script generated for node SQL Query
SqlQuery0 = '''
select 
    stl.sensorReadingTime,
    stl.serialNumber,
    stl.distanceFromObject
from ct
left join stl 
on ct.serialNumber = stl.serialNumber
'''
SQLQuery_node1745599881360 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"ct":CustomerTrusted_node1745597669155, "stl":StepTrainerLanding_node1745597686931}, transformation_ctx = "SQLQuery_node1745599881360")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1745599881360, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745599846095", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1745600003753 = glueContext.getSink(path="s3://projects-0a2497c6-0bf0-44e1-b6f6-44c78604c8bc/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1745600003753")
AmazonS3_node1745600003753.setCatalogInfo(catalogDatabase="step_trainer_db",catalogTableName="step_trainer_trusted")
AmazonS3_node1745600003753.setFormat("glueparquet", compression="snappy")
AmazonS3_node1745600003753.writeFrame(SQLQuery_node1745599881360)
job.commit()