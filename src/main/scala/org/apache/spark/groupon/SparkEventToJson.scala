package org.apache.spark.groupon

import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.util.JsonProtocol
import org.json4s.JsonAST

/**
  * @author rxue
  * @since 11/25/17.
  */
object SparkEventToJson {
  def apply(event: SparkListenerEvent): JsonAST.JValue = JsonProtocol.sparkEventToJson(event)

}
