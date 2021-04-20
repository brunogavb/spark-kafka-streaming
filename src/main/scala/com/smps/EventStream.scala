package com.smps

import com.smps.util.Configs._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{LongType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.log4j.Logger

/**
 * Created by Bruno A. Canal da Silva on 17/09/2019.
 */
class EventStream(spark: SparkSession) {
  import spark.implicits._
  val log: Logger = Logger.getLogger(getClass.getName)

  def run(): Unit = {
    log.info(s"Product Name: $productName")
    log.info(s"Project Name: $projectName")
    log.info(s"Bootstrap Servers: $kafkaBootstrapServers")
    log.info(s"Topic Name: $eventHubTopic")
    log.info(s"EventHub Topic: $eventHubTopic")
    log.info(s"Offset Start: $eventHubStartingOffset")
    val eventDataFrame = consumerEvent(spark)
    prepareInputDataframe(eventDataFrame)
      .writeStream
      .foreachBatch(
        function = (batchDS: Dataset[Row], batchId: Long) => processMicroBatch(batchDS, batchId)
      )
      .trigger(Trigger.Once())
      .option("checkpointLocation", s"$checkpointLocation/$relativePath")
      .start()
      .awaitTermination()
  }

  def processMicroBatch(batchDS: Dataset[Row], batchId: Long): Unit = {
    log.info(s"Processing micro-batch: $batchId")
    val schema = getSchemaFromDataset(batchDS)
    log.info(s"trying process: $batchId")
    try {
      if (!batchDS.isEmpty) {
        log.info(s"writing ${batchDS.count.toString} rows" )
        batchDS
          .coalesce(1)
          .withColumn("payload", from_json('body, schema)).drop('body)
          .write
          .partitionBy("year", "month", "day")
          .format("avro")
          .mode(SaveMode.Append)
          .save(s"$dataLocation/$relativePath")
        } else {
        log.info("micro-batch is empty")
      }
      } catch {
        case e: Exception => log.error(e.getMessage)
        System.exit(1);
    }
  }

  def getSchemaFromDataset(ds: Dataset[Row]): StructType = {
    log.info("generating schema")
    val temp = ds.select('body).rdd.map(x => x.getAs[String]("body")).toDS
    log.info("schema generated")
    log.info("returning schema generated")
    val schema = spark.read.json(temp).schema
    schema
  }

  def consumerEvent(spark: SparkSession): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("subscribe", eventHubTopic)
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.jaas.config", kafkaSasl)
      .option("kafka.allow.auto.create.topics", true)
      .option("kafka.request.timeout.ms", eventHubRequestTimeout)
      .option("kafka.session.timeout.ms", eventHubSessionTimeout)
      .option("startingOffsets", eventHubStartingOffset)
      .option("failOnDataLoss", "false")
      .load()
  }

  def prepareInputDataframe(df: DataFrame): DataFrame =
    df.withColumn("offset", 'offset.cast(LongType))
      .withColumn("timestamp", 'timestamp.cast(TimestampType))
      .withColumn("year", year('timestamp))
      .withColumn("month", month('timestamp))
      .withColumn("day", dayofmonth('timestamp))
      .withColumn("body", regexp_replace('value.cast(StringType), "\\$|\\\\", ""))
      .withColumn("body", regexp_replace('body, "\"\\{", "{"))
      .withColumn("body", regexp_replace('body, "\\}\"", "}"))
      .select('offset, 'timestamp, 'year, 'month, 'day, 'body)

}
