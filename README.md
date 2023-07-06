#### EMR on EC2
* python lib venv
* emr 6.7.0
```shell
# python lib 
export s3_location=s3://panchao-data/tmp/log
deactivate
mkdir app_log && cd app_log
rm -rf ./log_venv
rm -rf ./log_util_*.whl
rm -rf ./log_venv.tar.gz

python3 -m venv log_venv
source log_venv/bin/activate
pip3 install --upgrade pip
pip3 install redshift_connector jproperties pandas  sqlalchemy==1.4.46 pymysql
# log_util是封装好的Spark Redshift的包，源代码在log_util中
wget https://dxs9dnjebzm6y.cloudfront.net/tmp/log_util_202307061439-1.0-py3-none-any.whl

pip3 install log_util_202307061439-1.0-py3-none-any.whl
pip3 install venv-pack
venv-pack -f -o log_venv.tar.gz
# 上传到S3
aws s3 cp log_venv.tar.gz ${s3_location}/
```
* depedency jars
```shell
# kafka lib
export s3_location=s3://panchao-data/tmp/log
aws s3 rm --recursive ${s3_location}/jars/
mkdir jars
wget -P ./jars  https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.2.1/spark-sql-kafka-0-10_2.12-3.2.1.jar
wget -P ./jars  https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.8.2/kafka-clients-2.8.2.jar
wget -P ./jars  https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.2.1/spark-token-provider-kafka-0-10_2.12-3.2.1.jar
wget -P ./jars  https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
aws s3 sync ./jars ${s3_location}/jars/

# job lib
rm -rf emr-spark-redshift-1.2-*.jar
rm -rf spark-sql-kafka-offset-committer-*.jar
wget https://dxs9dnjebzm6y.cloudfront.net/tmp/emr-spark-redshift-1.2-SNAPSHOT-07030146.jar
wget https://dxs9dnjebzm6y.cloudfront.net/tmp/spark-sql-kafka-offset-committer-1.0.jar

aws s3 cp emr-spark-redshift-1.2-SNAPSHOT-07030146.jar  ${s3_location}/
aws s3 cp spark-sql-kafka-offset-committer-1.0.jar  ${s3_location}/

```
* submit job
```shell
export s3_location=s3://panchao-data/tmp/log
# cluster mode
# 代码中的emr_ec2中log_redshift.py和conf的log-job-ec2.properties
rm -rf log_redshift*.py 
rm -rf log-job-ec2*.properties
wget https://raw.githubusercontent.com/yhyyz/app-log-redshift/main/emr_ec2/log_redshift.py
wget https://raw.githubusercontent.com/yhyyz/app-log-redshift/main/config/log-job-ec2.properties
aws s3 cp log_redshift.py  ${s3_location}/
aws s3 cp log-job-ec2.properties  ${s3_location}/

spark-submit --master yarn --deploy-mode cluster \
--num-executors 5 \
--conf "spark.yarn.dist.archives=${s3_location}/log_venv.tar.gz#log_venv" \
--conf "spark.pyspark.python=./log_venv/bin/python" \
--conf spark.speculation=false \
--conf spark.sql.streaming.streamingQueryListeners=net.heartsavior.spark.KafkaOffsetCommitterListener \
--conf spark.executor.cores=4 \
--conf spark.executor.memory=8g \
--conf spark.driver.cores=4 \
--conf spark.driver.memory=8g \
--conf spark.sql.shuffle.partitions=2 \
--conf spark.default.parallelism=2 \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.jars=${s3_location}/emr-spark-redshift-1.2-SNAPSHOT-07030146.jar,${s3_location}/spark-sql-kafka-offset-committer-1.0.jar,${s3_location}/jars/*.jar,/usr/share/aws/redshift/jdbc/RedshiftJDBC.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-avro.jar,/usr/share/aws/redshift/spark-redshift/lib/minimal-json.jar \
${s3_location}/log_redshift.py us-east-1 ${s3_location}/log-job-ec2.properties


# client mode
export s3_location=s3://panchao-data/tmp/log
spark-submit --master yarn --deploy-mode client \
--num-executors 5 \
--conf "spark.yarn.dist.archives=${s3_location}/log_venv.tar.gz#log_venv" \
--conf "spark.pyspark.python=./log_venv/bin/python" \
--conf "spark.pyspark.driver.python=./log_venv/bin/python" \
--conf spark.speculation=false \
--conf spark.sql.streaming.streamingQueryListeners=net.heartsavior.spark.KafkaOffsetCommitterListener \
--conf spark.executor.cores=4 \
--conf spark.executor.memory=8g \
--conf spark.driver.cores=4 \
--conf spark.driver.memory=8g \
--conf spark.sql.shuffle.partitions=2 \
--conf spark.default.parallelism=2 \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.jars=${s3_location}/emr-spark-redshift-1.2-SNAPSHOT-07030146.jar,${s3_location}/spark-sql-kafka-offset-committer-1.0.jar,${s3_location}/jars/commons-pool2-2.11.1.jar,${s3_location}/jars/kafka-clients-2.8.2.jar,${s3_location}/jars/spark-sql-kafka-0-10_2.12-3.2.1.jar,${s3_location}/jars/spark-token-provider-kafka-0-10_2.12-3.2.1.jar,/usr/share/aws/redshift/jdbc/RedshiftJDBC.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-avro.jar,/usr/share/aws/redshift/spark-redshift/lib/minimal-json.jar \
${s3_location}/log_redshift.py us-east-1 ${s3_location}/log-job-ec2.properties

```
