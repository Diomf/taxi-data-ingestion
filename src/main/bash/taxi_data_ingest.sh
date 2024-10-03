#!/usr/bin/env bash

function data_ingestion {
  SPARK_SUBMIT_PARENT_PATH=${SPARK_SUBMIT_PARENT_PATH:="."}
  SPARK_MASTER=${SPARK_MASTER:="local[*]"}  # spark://<spark_master_ip>:7077
  SPARK_EXECUTOR_NUM=${SPARK_EXECUTOR_NUM:=2}
  SPARK_EXECUTOR_CORE_NUM=${SPARK_EXECUTOR_CORE_NUM:=2}
  SPARK_EXECUTOR_MEM=${SPARK_EXECUTOR_MEM:="4g"}
  DRIVER_CLASS=${DRIVER_CLASS:="com.mobito.bdi.TaxiDataIngestion"}
  MAIN_JAR=${MAIN_JAR:="target\scala-2.12\taxi-data-ingestion_2.12-0.1.0.jar"}

 echo "Running spark-submit..."
  ${SPARK_SUBMIT_PARENT_PATH}/spark-submit --master ${SPARK_MASTER} \
  --driver-memory 4g \
  --conf "spark.executor.memory=${SPARK_EXECUTOR_MEM}" \
  --conf "spark.executor.memoryOverhead=4g" \
  --num-executors ${SPARK_EXECUTOR_NUM} \
  --executor-cores ${SPARK_EXECUTOR_CORE_NUM} \
  --packages org.rogach:scallop_2.12:3.0.3,org.apache.hadoop:hadoop-aws:3.2.0 \
  --class ${DRIVER_CLASS} \
  ${MAIN_JAR} ${@}

  RESULT="$?"

  if [ "$RESULT" != "0" ]; then
    echo -e "Exit Code: $RESULT."
    exit ${RESULT}
  fi
}

args=${@}
echo "Running taxi-data-ingestion. Time : `date +%Y/%m/%d\ %H:%M:%S`"
data_ingestion "${args}"
echo "Successful taxi-data-ingestion. Time : `date +%Y/%m/%d\ %H:%M:%S`"



