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

# Script generated for node Customer Landing
CustomerLanding_node1711828132092 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://prayat-stedi-project/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLanding_node1711828132092")

# Script generated for node SQL Query
SqlQuery1948 = '''
select * from myDataSource
where myDataSource.sharewithresearchasofdate is not null

'''
SQLQuery_node1711829376319 = sparkSqlQuery(glueContext, query = SqlQuery1948, mapping = {"myDataSource":CustomerLanding_node1711828132092}, transformation_ctx = "SQLQuery_node1711829376319")

# Script generated for node Customer Trusted
CustomerTrusted_node1711828344725 = glueContext.getSink(path="s3://prayat-stedi-project/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1711828344725")
CustomerTrusted_node1711828344725.setCatalogInfo(catalogDatabase="stedi-human-balance-analytics",catalogTableName="customer_trusted")
CustomerTrusted_node1711828344725.setFormat("json")
CustomerTrusted_node1711828344725.writeFrame(SQLQuery_node1711829376319)
job.commit()