import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
import re

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

# Script generated for node Customer Landing Json
CustomerLandingJson_node1745594539080 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://projects-0a2497c6-0bf0-44e1-b6f6-44c78604c8bc/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLandingJson_node1745594539080")

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node1745594627331 = Filter.apply(frame=CustomerLandingJson_node1745594539080, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="CustomerPrivacyFilter_node1745594627331")

# Script generated for node Customer Trusted Parquet
EvaluateDataQuality().process_rows(frame=CustomerPrivacyFilter_node1745594627331, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745594488038", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrustedParquet_node1745594846997 = glueContext.getSink(path="s3://projects-0a2497c6-0bf0-44e1-b6f6-44c78604c8bc/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrustedParquet_node1745594846997")
CustomerTrustedParquet_node1745594846997.setCatalogInfo(catalogDatabase="step_trainer_db",catalogTableName="customer_trusted")
CustomerTrustedParquet_node1745594846997.setFormat("glueparquet", compression="snappy")
CustomerTrustedParquet_node1745594846997.writeFrame(CustomerPrivacyFilter_node1745594627331)
job.commit()