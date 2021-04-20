package com.smps

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite

import scala.util.Try

/**
 * Created by Renan T. Pinzon on 07/26/2019.
 */
abstract class UnitSparkSpec extends FunSuite {

  protected lazy val spark: SparkSession = {
    val sparkSession = SparkSession.builder()
      .config(sparkConf)
      .master("local")
      .getOrCreate()

    sparkSession.conf.set("spark.sql.tungsten.enabled", "true")
    sparkSession
  }

  private lazy val sparkConf = {
    val conf = new SparkConf()
    conf.set("spark.memory.offHeap.enabled", "false")
  }

  def pathToLayouts(filename: String) = s"src/test/resources/$filename"

  def hasColumn(df: DataFrame, path: String): Boolean = Try(df(path)).isSuccess

}
