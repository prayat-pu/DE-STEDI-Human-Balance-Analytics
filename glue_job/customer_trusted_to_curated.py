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

# Script generated for node Customer Trusted
CustomerTrusted_node1711831330685 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://prayat-stedi-project/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1711831330685")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1711831490754 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://prayat-stedi-project/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1711831490754")

# Script generated for node SQL Query
SqlQuery1751 = '''
select distinct customerName, email, phone, birthDay, serialNumber, registrationDate, lastUpdateDate, shareWithResearchAsOfDate, shareWithPublicAsOfDate, shareWithFriendsAsOfDate 
from customer 
join accelerometer on customer.email = accelerometer.user

'''
SQLQuery_node1711831829317 = sparkSqlQuery(glueContext, query = SqlQuery1751, mapping = {"customer":CustomerTrusted_node1711831330685, "accelerometer":AccelerometerTrusted_node1711831490754}, transformation_ctx = "SQLQuery_node1711831829317")

# Script generated for node Amazon S3
AmazonS3_node1711831653744 = glueContext.getSink(path="s3://prayat-stedi-project/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1711831653744")
AmazonS3_node1711831653744.setCatalogInfo(catalogDatabase="stedi-human-balance-analytics",catalogTableName="customer_curated")
AmazonS3_node1711831653744.setFormat("json")
AmazonS3_node1711831653744.writeFrame(SQLQuery_node1711831829317)
job.commit()