from botocore.exceptions import ClientError
import boto3
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql.functions import col,to_timestamp,to_date,date_add,expr
import redshift_connector
import json
from typing import Optional
import base64
import re


def gen_filter_udf(event_name):
    def filter_table(str_json, ):
        event = '"event":"{0}"'.format(event_name)
        event_pattern = re.compile(event)
        event_res = event_pattern.findall(str_json)
        if event_res:
            return True
        else:
            return False
    return udf(filter_table, BooleanType())


class LOGRedshiftSink:
    def __init__(self, spark, redshift_schema, redshift_iam_role, redshift_tmpdir, logger=None,
                 disable_dataframe_show="false", host: Optional[str] = None, port: Optional[int] = None,
                 database: Optional[str] = None, user: Optional[str] = None,
                 password: Optional[str] = None, redshift_secret_id: Optional[str] = None,
                 region_name: Optional[str] = None, s3_endpoint: Optional[str] = None, tempformat: Optional[str] = None):
        if logger:
            self.logger = logger
        else:
            self.logger = print
        self.disable_dataframe_show = disable_dataframe_show
        self.data_frame = None
        self.spark = spark
        self.s3_endpoint = s3_endpoint
        if tempformat:
            self.tempformat = tempformat
        else:
            self.tempformat = "CSV"

        self.redshift_tmpdir = redshift_tmpdir
        self.redshift_iam_role = redshift_iam_role

        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.redshift_schema = redshift_schema
        self.redshift_secret_id = redshift_secret_id
        self.region_name = region_name

        if redshift_secret_id:
            secret_dict = json.loads(self._get_secret())
            self.con = redshift_connector.connect(
                host=secret_dict["host"],
                database=secret_dict["database"],
                user=secret_dict["username"],
                password=secret_dict["password"],
                port=int(secret_dict["port"])
            )
            self.host = secret_dict["host"]
            self.database = secret_dict["database"]
            self.user = secret_dict["username"]
            self.port = int(secret_dict["port"])
            self.password = secret_dict["password"]

        else:
            self.con = redshift_connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=int(self.port)
            )

    def _getDFExampleString(self, df):
        if self.disable_dataframe_show == "false":
            data_str = df._jdf.showString(5, 20, False)
            # truncate false
            # data_str = df._jdf.showString(5, int(false), False)
            schema_str = df._jdf.schema().treeString()
            return schema_str + "\n" + data_str
        else:
            return "(disable show dataframe)"
    def _getDFSchemaJsonString(self, df):
        if self.disable_dataframe_show == "false":
            schema_str = df.schema.json()
            return schema_str
        else:
            return "(disable show dataframe schema)"

    def _run_sql_with_result(self, sql_str, schema):
        with self.con.cursor() as cursor:
            cursor.execute("set search_path to '$user', public, {0}".format(schema))
            cursor.execute(sql_str)
            res = cursor.fetchall()
            return res

    def _do_write(self, scf, event_field_list, redshift_schema, target_table):
        redshift_target_table = redshift_schema + "." + target_table
        view_name = "kafka_source_" + target_table
        scf.createOrReplaceGlobalTempView(view_name)

        df = self.spark.sql("select * from {view_name}".format(view_name=view_name))
        df_outer_field_dict = {}
        df_prop_field_dict = {}
        outer_column_list = []
        prop_column_list = []
        for field in df.schema.fields:
            data_type = str(field.dataType)
            column_name = field.name
            first_10_chars = data_type[0:10]
            if first_10_chars == "StructType(":
                current_col = column_name
                properties_data_type_json_str = df.schema[current_col].dataType.json()
                properties_type_json = json.loads(properties_data_type_json_str)
                # [{'metadata': {}, 'name': 'ad_event_id', 'nullable': True, 'type': 'string'}, {'metadata': {}....}...]
                fields_list = properties_type_json["fields"]
                for df_prop_field in fields_list:
                    filed_name = df_prop_field["name"]
                    filed_type = df_prop_field["type"]
                    df_prop_field_dict[filed_name] = filed_type
            else:
                df_outer_field_dict[column_name] = data_type
        asn_column = ""
        for event_field in event_field_list:
            if "asn:" in event_field:
                asn_column = event_field
                break
        asn_index = event_field_list.index(asn_column)
        outer_field_list = event_field_list[:asn_index+1]
        properties_field_list = event_field_list[asn_index+1:]
        for outer_field in outer_field_list:
            tmp = outer_field.split(":")
            field = tmp[0]
            field_type = tmp[1]
            if field in df_outer_field_dict:
                sql = "cast({column_name} as {column_type})".format(column_name=field,column_type=field_type)
                outer_column_list.append(sql)
        for prop_field in properties_field_list:
            tmp = prop_field.split(":")
            field = tmp[0]
            field_type = tmp[1]
            filed_sql_col = "cast(properties_raw.{prop_field} as {column_type}) as {prop_field}".format(prop_field=field, column_type=field_type)
            prop_column_list.append(filed_sql_col)
        select_column_sql = ",".join(outer_column_list+prop_column_list)
        self.logger("gen columns from metadata: {}".format(select_column_sql))
        df = self.spark.sql("select {column_list} from {view_name}".format(column_list=select_column_sql, view_name=view_name))
        self.logger("event dataframe spark write to s3 {0}".format(self._getDFExampleString(df)))
        df.write \
            .format("io.github.spark_redshift_community.spark.redshift") \
            .option("url", "jdbc:redshift://{0}:{1}/{2}".format(self.host, self.port, self.database)) \
            .option("dbtable", redshift_target_table) \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("tempdir", self.redshift_tmpdir) \
            .option("tempformat", self.tempformat) \
            .option("s3_endpoint", self.s3_endpoint) \
            .option("extracopyoptions", "TRUNCATECOLUMNS region '{0}' dateformat 'auto' timeformat 'auto'".format(self.region_name)) \
            .option("aws_iam_role", self.redshift_iam_role).mode("append").save()

    def run_task(self, event_name, event_field_list, data_frame):
        task_status = {}
        try:
            self.logger("sync event_name info:" + str(event_name))
            target_table = event_name
            task_status["table_name"] = target_table

            df = data_frame.filter(gen_filter_udf(event_name)(col('value')))
            # self.logger("the table {0}: record number: {1}".format(table_name, str(fdf.count())))
            if not df.rdd.isEmpty():
                self.logger("the table {0}:  kafka source data: {1}".format(target_table, self._getDFExampleString(df)))
                # auto gen schema
                json_schema = self.spark.read.json(df.rdd.map(lambda p: str(p["value"]))).schema
                self.logger("the table {0}: auto gen json schema: {1}".format(target_table, str(json_schema)))
                scf = df.select(from_json(col("value"), json_schema).alias("kdata")).select("kdata.*")

                self.logger("the table {0}: kafka source data with auto gen schema: {1}".format(target_table,
                                                                                                self._getDFExampleString(
                                                                                                    scf)))
                self._do_write(scf, event_field_list, self.redshift_schema, target_table)

                self.logger("sync the table complete: " + target_table)
                task_status["status"] = "finished"
                return task_status
            else:
                task_status["status"] = "the table in the current batch has no data"
        except Exception as e:
            task_status["status"] = "error"
            task_status["exception"] = "{0}".format(e)
            self.logger(e)
            return task_status

    def _get_secret(self):
        secret_name = self.redshift_secret_id
        region_name = self.region_name
        self.logger(
            "get redshift conn from secrets manager,secret_id: {0} region_name: {1}".format(secret_name, region_name))
        # Create a Secrets Manager client

        session = boto3.session.Session(region_name=region_name)
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            raise e
        else:
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
                return secret
            else:
                decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
                return decoded_binary_secret
