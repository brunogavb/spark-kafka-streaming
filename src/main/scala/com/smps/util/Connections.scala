package com.smps.util

import com.smps.util.Configs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object Connections {

  lazy val getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("consumer-ms")
      .setMaster("local[*]")
      .set("spark.sql.tungsten.enabled", "true")
      .set("spark.memory.offHeap.enabled", "false")
      .set("fs.defaultFS", adlsDefaultFS)
      .set("fs.azure.account.auth.type", "OAuth")
      .set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
      .set("fs.azure.account.oauth2.client.id", adlsClientId)
      .set("fs.azure.account.oauth2.client.secret", adlsClientSecret)
      .set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/" + adlsTenantId + "/oauth2/token")
      .set("spark.authenticate", "true")
      .set("spark.authenticate.secret", sparkSecret)
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    spark.sparkContext
      .hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    spark
  }

  lazy val getHadoopFileSystem: FileSystem = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", adlsDefaultFS)
    conf.set("fs.azure.account.auth.type", "OAuth")
    conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    conf.set("fs.azure.account.oauth2.client.id", adlsClientId)
    conf.set("fs.azure.account.oauth2.client.secret", adlsClientSecret)
    conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/" + adlsTenantId + "/oauth2/token")
    FileSystem.get(conf)
  }

}
