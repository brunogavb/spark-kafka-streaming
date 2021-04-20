package com.smps.testes

import scala.collection.mutable

object Definition {
  case class FieldDefinition(column_name: String, column_type: String, position: Int)
  case class TableDefinition(table_name: String, source_dir: String, delimiter: String, key: String, quote: String, columns: mutable.Map[Int, FieldDefinition]) {

    @transient def toJson: String = {
      val sb = new StringBuilder()
      sb.append("{") // root
      sb.append("\"table_name\": \"").append(stringOrNUll(table_name)).append("\",")
      sb.append("\"source_dir\": \"").append(stringOrNUll(source_dir)).append("\",")
      sb.append("\"delimiter\": \"").append(stringOrNUll(delimiter)).append("\",")
      sb.append("\"key\": \"").append(stringOrNUll(key)).append("\",")
      sb.append("\"quote\": \"").append(stringOrNUll(quote)).append("\",")
      sb.append("\"columns\": [").append(columnsToJson).append("]")
      sb.append("}") // root
      sb.toString
    }

    @transient private def columnsToJson: String = {
      val sb = new StringBuilder()
      columns.foreach({ case (position, fieldDefinition) =>
        sb.append("{") // column
        sb.append("\"column_name\": \"").append(stringOrNUll(fieldDefinition.column_name)).append("\",")
        sb.append("\"column_type\": \"").append(stringOrNUll(fieldDefinition.column_type)).append("\",")
        sb.append("\"position\": \"").append(position).append("\"")
        sb.append("},") // column
      })
      if (columns.nonEmpty) sb.deleteCharAt(sb.length - 1) // remove last comma from columns
      sb.toString
    }

    @transient private def stringOrNUll(value: String): String = {
      if (value == null || value.isEmpty || "null" == value) null else value
    }


  }



}
