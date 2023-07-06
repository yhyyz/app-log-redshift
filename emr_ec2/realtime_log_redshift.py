import sys
from pyspark.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import boto3
from jproperties import Properties
from urllib.parse import urlparse
from log_util.realtime_redshift_sink import RealtimeLogRedshiftSink
from io import StringIO
import pandas as pd
from sqlalchemy import create_engine

"""
if need to restart job and consume data from kafka earliest, please rm checkpoint dir 
which define in job.properties, otherwise job restart from checkpoint
"""

aws_region = ""
config_s3_path = ""
job_name = "app-realtime-log-redshift"
if len(sys.argv) > 1:
    aws_region = sys.argv[1]
    config_s3_path = sys.argv[2]
else:
    print("Job failed. Please provided params aws_region,config_s3_path")
    sys.exit(1)

args = sys.argv

spark = SparkSession.builder.config('spark.scheduler.mode', 'FAIR').getOrCreate()
sc = spark.sparkContext
log4j = sc._jvm.org.apache.log4j
logger = log4j.LogManager.getLogger(__name__)


def load_config(aws_region, config_s3_path):
    o = urlparse(config_s3_path, allow_fragments=False)
    client = boto3.client('s3', region_name=aws_region)
    data = client.get_object(Bucket=o.netloc, Key=o.path.lstrip('/'))
    contents = data['Body'].read().decode("utf-8")
    configs = Properties()
    configs.load(contents)
    return configs


params = load_config(aws_region.strip(), config_s3_path.strip())

s = StringIO()
params.list(out_stream=s)
logger.info("load config from s3 - my_log - params: {0}".format(s.getvalue()))
if not params:
    raise Exception("load config error  - my_log - s3_path: {0}".format(config_s3_path))


aws_region = params["aws_region"].data
s3_endpoint = params["s3_endpoint"].data
checkpoint_location = params["checkpoint_location"].data
checkpoint_interval = params["checkpoint_interval"].data
kafka_broker = params["kafka_broker"].data
topic = params["topic"].data
startingOffsets = params["startingOffsets"].data
thread_max_workers = int(params["thread_max_workers"].data)
disable_msg = params["disable_msg"].data
max_offsets_per_trigger = params["max_offsets_per_trigger"].data
consumer_group = params["consumer_group"].data

tempformat = "JSON"
tempformat_p = params.get("tempformat")
if tempformat_p:
    tempformat = tempformat_p.data

sync_table_list = json.loads(params["sync_table_list"].data)
redshift_secret_id = params["redshift_secret_id"].data
redshift_host = params["redshift_host"].data
redshift_port = int(params["redshift_port"].data)
redshift_username = params["redshift_username"].data
redshift_password = params["redshift_password"].data
redshift_database = params["redshift_database"].data
redshift_schema = params["redshift_schema"].data
redshift_tmpdir = params["redshift_tmpdir"].data
redshift_iam_role = params["redshift_iam_role"].data

metadata_host = params["metadata_host"].data
metadata_port = params["metadata_port"].data
metadata_username = params["metadata_username"].data
metadata_password = params["metadata_password"].data
metadata_database = params["metadata_database"].data
metadata_event_type = str(params["metadata_event_type"].data)
target_table=params["target_table"].data

reader = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic) \
    .option("maxOffsetsPerTrigger", max_offsets_per_trigger) \
    .option("kafka.consumer.commit.groupid", consumer_group)
if startingOffsets == "earliest" or startingOffsets == "latest":
    reader.option("startingOffsets", startingOffsets)
else:
    reader.option("startingTimestamp", startingOffsets)
kafka_data = reader.load()

source_data = kafka_data.selectExpr("CAST(value AS STRING)")


def logger_msg(msg):
    if disable_msg == "false":
        logger.info(job_name + " - my_log - {0}".format(msg))
    else:
        pass
def get_realtime_events_metadata():
    engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}"
                           .format(metadata_username,
                                   metadata_password,
                                   metadata_host,
                                   metadata_port,
                                   metadata_database,
                                   'utf8')
                           )

    field_sql = """
    select concat_ws(':', field_name, data_type) as field_name,json_field_name  from realtime_events_event_field_list  order by id
    """
    field_df = pd.read_sql_query(field_sql, engine)
    field_dict = field_df.to_dict()
    filed_list = []
    event_list = []
    for idx in range(len(field_dict["field_name"])):
        filed_list.append(field_dict["field_name"][idx])

    event_sql = "select event_name,event_type from realtime_events_event_list where event_type in ({event_type})  order by id".format(event_type=metadata_event_type)
    event_df = pd.read_sql_query(event_sql, engine)
    event_dict = event_df.to_dict()
    for idx in range(len(event_dict["event_name"])):
        event_list.append(event_dict["event_name"][idx])
    return event_list, filed_list


event_list,field_list = get_realtime_events_metadata()


def process_batch(data_frame, batchId):
    dfc = data_frame.cache()
    logger.info(job_name + " - my_log - process batch id: " + str(batchId) + " record number: " + str(dfc.count()))
    if not data_frame.rdd.isEmpty() > 0:
        rs = RealtimeLogRedshiftSink(spark, redshift_schema, redshift_iam_role, redshift_tmpdir,
                             logger=logger_msg, disable_dataframe_show=disable_msg, host=redshift_host,
                             port=redshift_port, database=redshift_database, user=redshift_username,
                             password=redshift_password, redshift_secret_id=redshift_secret_id, region_name=aws_region,
                             s3_endpoint=s3_endpoint, tempformat=tempformat)
        rs.run_task(event_list,field_list,target_table, dfc)
        dfc.unpersist()
        logger.info(job_name + " - my_log - finish batch id: " + str(batchId))

save_to_redshift = source_data \
    .writeStream \
    .outputMode("append") \
    .trigger(processingTime=checkpoint_interval) \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", checkpoint_location) \
    .start()

save_to_redshift.awaitTermination()