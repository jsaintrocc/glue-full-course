import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer DB
CustomerDB_node1721406636623 = glueContext.create_dynamic_frame.from_catalog(database="customers_database", table_name="customers_csv", transformation_ctx="CustomerDB_node1721406636623")

# Script generated for node Change Schema
ChangeSchema_node1721406644607 = ApplyMapping.apply(frame=CustomerDB_node1721406636623, mappings=[("customerid", "long", "customerid", "string"), ("namestyle", "boolean", "namestyle", "string"), ("title", "string", "title", "string"), ("firstname", "string", "firstname", "string"), ("middlename", "string", "middlename", "string"), ("lastname", "string", "lastname", "string"), ("suffix", "string", "suffix", "string"), ("companyname", "string", "companyname", "string"), ("salesperson", "string", "salesperson", "string"), ("emailaddress", "string", "emailaddress", "string"), ("phone", "string", "phone", "string"), ("passwordhash", "string", "passwordhash", "string"), ("passwordsalt", "string", "passwordsalt", "string"), ("rowguid", "string", "rowguid", "string"), ("modifieddate", "string", "modifieddate", "string"), ("dataload", "string", "dataload", "string")], transformation_ctx="ChangeSchema_node1721406644607")

# Script generated for node Amazon S3
AmazonS3_node1721407671263 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1721406644607, connection_type="s3", format="glueparquet", connection_options={"path": "s3://jsr-glue-full-course/data/customers_database/customer_parquet/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1721407671263")

job.commit()