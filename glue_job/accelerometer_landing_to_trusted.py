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

# Script generated for node Trusted Customer
TrustedCustomer_node1711829886350 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://prayat-stedi-project/customer/trusted/"], "recurse": True}, transformation_ctx="TrustedCustomer_node1711829886350")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1711830000889 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://prayat-stedi-project/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1711830000889")

# Script generated for node SQL Query
SqlQuery1635 = '''
select accelerometer.user, accelerometer.timestamp, accelerometer.x, accelerometer.y, accelerometer.z 
from customer join accelerometer on customer.email = accelerometer.user

'''
SQLQuery_node1711830847044 = sparkSqlQuery(glueContext, query = SqlQuery1635, mapping = {"customer":TrustedCustomer_node1711829886350, "accelerometer":AccelerometerLanding_node1711830000889}, transformation_ctx = "SQLQuery_node1711830847044")

# Script generated for node Amazon S3
AmazonS3_node1711830371049 = glueContext.getSink(path="s3://prayat-stedi-project/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1711830371049")
AmazonS3_node1711830371049.setCatalogInfo(catalogDatabase="stedi-human-balance-analytics",catalogTableName="acceleometer_trusted")
AmazonS3_node1711830371049.setFormat("json")
AmazonS3_node1711830371049.writeFrame(SQLQuery_node1711830847044)
job.commit()