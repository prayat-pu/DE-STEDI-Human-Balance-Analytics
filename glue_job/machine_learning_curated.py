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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1711833993706 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://prayat-stedi-project/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1711833993706")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1711833838817 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://prayat-stedi-project/step_trainer/trusted/"], "recurse": True}, transformation_ctx="StepTrainerTrusted_node1711833838817")

# Script generated for node SQL Query
SqlQuery1797 = '''
select  trainer.serialNumber, trainer.sensorReadingTime, trainer.distanceFromObject, accelerometer.user, accelerometer.x, accelerometer.y, accelerometer.z
from trainer
join accelerometer on trainer.
sensorReadingTime = accelerometer.timestamp
'''
SQLQuery_node1711834046863 = sparkSqlQuery(glueContext, query = SqlQuery1797, mapping = {"accelerometer":AccelerometerTrusted_node1711833993706, "trainer":StepTrainerTrusted_node1711833838817}, transformation_ctx = "SQLQuery_node1711834046863")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1711834246196 = glueContext.getSink(path="s3://prayat-stedi-project/machine-learning-curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1711834246196")
MachineLearningCurated_node1711834246196.setCatalogInfo(catalogDatabase="stedi-human-balance-analytics",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1711834246196.setFormat("json")
MachineLearningCurated_node1711834246196.writeFrame(SQLQuery_node1711834046863)
job.commit()