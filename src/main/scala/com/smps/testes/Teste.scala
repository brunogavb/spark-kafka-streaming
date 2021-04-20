package com.smps.testes

import com.google.gson.{Gson, JsonObject, JsonParser}
import org.apache.spark.sql.SparkSession
import com.smps.testes.Definition.{FieldDefinition, TableDefinition}

import scala.collection.mutable

object Teste extends App {

  val spark = SparkSession.builder.appName("OPA").master("local[1]").getOrCreate()

  def getColumns(): mutable.Map[Int, FieldDefinition] = {
    var position = 0
    val fields = mutable.Map[Int, FieldDefinition]().empty

    val df = spark.sql(
      """
      SELECT 1 as ID, 'row 1' as NAME
      """)

    val jsonAsString: JsonObject = new JsonParser().parse(df.schema.json).getAsJsonObject
    val fieldsIterator = jsonAsString.get("fields").getAsJsonArray.iterator
    while (fieldsIterator.hasNext()) {
      val field = fieldsIterator.next().getAsJsonObject
      fields += position -> FieldDefinition(
        field.get("name").getAsString,
        field.get("type").getAsString,
        position
      )
      position = position+1
    }
    fields
  }

  val x = TableDefinition("nome_tabela", "source", ";", "xpto", "'", getColumns()).toJson

  println(x)

}

