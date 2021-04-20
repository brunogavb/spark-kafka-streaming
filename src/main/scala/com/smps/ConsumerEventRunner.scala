package com.smps

import com.smps.util.Connections.getSparkSession

object ConsumerEventRunner {

  def main(args: Array[String]): Unit = {
    try {
      new EventStream(getSparkSession).run()
    } catch {
      case e: Exception => println(e.getStackTrace)
    }
  }

}
