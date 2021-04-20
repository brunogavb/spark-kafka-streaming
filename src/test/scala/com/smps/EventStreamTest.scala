package com.smps

import org.apache.spark.sql.types._
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfter


class EventStreamTest extends UnitSparkSpec with BeforeAndAfter {

  import spark.implicits._

  val eventStream = spy(new EventStream(spark))

  test("Event from stream is prepared for processing (casting types and adding new columns) ") {

    val input = Seq(
      ("1", "2019-08-01T17:22:31+00:00", "{}")
    ).toDF("offset", "timestamp", "value")

    val result = eventStream.prepareInputDataframe(input)

    val expectedSchema = new StructType()
      .add("offset", LongType)
      .add("timestamp", TimestampType)
      .add("year", IntegerType)
      .add("month", IntegerType)
      .add("day", IntegerType)
      .add("body", StringType)

    assertResult(expectedSchema, "Result schema do not match expected schema") {
      result.schema
    }
  }

  test("Getting schema from body column") {

    val input = Seq(
      ("""{"a": "value", "b": 0}""")
    ).toDF("body")

    val expected = new StructType().add("a", StringType).add("b", LongType)

    assertResult(expected, "Generated schema do not match expected schema") {
      eventStream.getSchemaFromDataset(input)
    }
  }

}
