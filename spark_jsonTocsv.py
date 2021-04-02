from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, concat
from pyspark.sql.types import *
import botocore.session

session = botocore.session.get_session()
credentials = session.get_credentials()

spark = (
    SparkSession
    .builder
    .config('spark.hadoop.fs.s3a.impl',
           'org.apache.hadoop.fs.s3a.S3AFileSystem')
    .config(
        'spark.driver.extraClassPath', 
        '/home/ubuntu/aws-java-sdk-1.11.35.jar'
        '/home/ubuntu/hadoop-aws-2.8.4.jar')
    .config('fs.s3a.access.key', credentials.access_key)
    .config('fs.s3a.secret.key', credentials.secret_key)
    .appName("cluster")
    .getOrCreate()
)

########## Creating schema for dataframe ##############

schema = StructType([
        StructField('hash', StringType(), True),
        StructField('height', LongType(), True),
        StructField('time', LongType(), True),
        StructField('tx', ArrayType(
            StructType([
                StructField('hash', StringType(), True),
                StructField('txid', StringType(), True),
                StructField('vin', ArrayType(
                    StructType([
                        StructField('scriptSig', StructType([
                            StructField('asm', StringType(), True),
                            StructField('hex', StringType(), True),
                        ]), True),
                        StructField('txid', StringType(), True),
                        StructField('vout', LongType(), True),
                        StructField('txinwitness', ArrayType(
                            StringType()
                        ), True)
                    ])
                ), True),
                StructField('vout', ArrayType(
                    StructType([
                        StructField('n', LongType(), True),
                        StructField('scriptPubKey', StructType([
                            StructField('addresses', ArrayType(
                                StringType()
                            ), True),
                            StructField('type', StringType(), True)
                        ]), True),
                        StructField('value', DoubleType(), True)
                    ])
                ), True),
                StructField('weight', LongType(), True)
            ])
        ), True)
    ])


############# Reading json data and writing as csv file ##############

df = spark.read.json("s3a://blocks-data/*.json", multiLine=True, schema=schema) \
           .withColumn("tx", explode("tx")).coalesce(140)
df = df.select("height", "time")
df.write.csv("s3a://blocks-data/*.json, header=None, mode="append")