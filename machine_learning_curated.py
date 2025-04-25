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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1745602210570 = glueContext.create_dynamic_frame.from_catalog(database="step_trainer_db", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1745602210570")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1745602546875 = glueContext.create_dynamic_frame.from_catalog(database="step_trainer_db", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1745602546875")

# Script generated for node Customer Curated
CustomerCurated_node1745621588881 = glueContext.create_dynamic_frame.from_catalog(database="step_trainer_db", table_name="customers_curated", transformation_ctx="CustomerCurated_node1745621588881")

# Script generated for node SQL Query
SqlQuery0 = '''
with enriched_accelerometer as (
    select 
        a.*,
        c.serialnumber
    from accelerometer_trusted as a
    join customers_curated as c
    on a.user = c.email
)
select 
    ea.*,
    stt.distancefromobject
from step_trainer_trusted as stt
join enriched_accelerometer as ea
on ea.serialnumber = stt.serialnumber
and ea.timestamp = stt.sensorreadingtime
'''
SQLQuery_node1745621607960 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customers_curated":CustomerCurated_node1745621588881, "accelerometer_trusted":AccelerometerTrusted_node1745602210570, "step_trainer_trusted":StepTrainerTrusted_node1745602546875}, transformation_ctx = "SQLQuery_node1745621607960")

# Script generated for node machine_learning_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1745621607960, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745621140920", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machine_learning_curated_node1745621757034 = glueContext.getSink(path="s3://projects-0a2497c6-0bf0-44e1-b6f6-44c78604c8bc/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1745621757034")
machine_learning_curated_node1745621757034.setCatalogInfo(catalogDatabase="step_trainer_db",catalogTableName="machine_learning_curated")
machine_learning_curated_node1745621757034.setFormat("glueparquet", compression="snappy")
machine_learning_curated_node1745621757034.writeFrame(SQLQuery_node1745621607960)
job.commit()