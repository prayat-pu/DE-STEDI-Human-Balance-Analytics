import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1711832294221 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://prayat-stedi-project/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1711832294221")

# Script generated for node Customer Corated
CustomerCorated_node1711832422960 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://prayat-stedi-project/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCorated_node1711832422960")

# Script generated for node SQL Query
SqlQuery1572 = '''
select customer.serialNumber, sensorReadingTime, distanceFromObject  
from trainer
join customer on trainer.serialnumber = customer.serialnumber
'''
SQLQuery_node1711832645293 = sparkSqlQuery(glueContext, query = SqlQuery1572, mapping = {"trainer":StepTrainerLanding_node1711832294221, "customer":CustomerCorated_node1711832422960}, transformation_ctx = "SQLQuery_node1711832645293")

# Script generated for node Amazon S3
AmazonS3_node1711832740997 = glueContext.getSink(path="s3://prayat-stedi-project/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1711832740997")
AmazonS3_node1711832740997.setCatalogInfo(catalogDatabase="stedi-human-balance-analytics",catalogTableName="step_trainer_trusted")
AmazonS3_node1711832740997.setFormat("json")
AmazonS3_node1711832740997.writeFrame(SQLQuery_node1711832645293)
job.commit()