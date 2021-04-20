package com.smps.util

import org.apache.avro.Schema
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.StructType

object Schemas {

  def getFileSchema(pathSchema: String) = {
    val inputStream = Connections.getHadoopFileSystem.open(new Path(pathSchema))
    new Schema.Parser().parse(inputStream)
  }

}
