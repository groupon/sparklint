package org.apache.spark.groupon

import org.apache.spark.scheduler.{SparkListenerEvent, SparkListenerLogStart}
import org.apache.spark.util.JsonProtocol
import org.json4s.jackson.JsonMethods.parse

/**
  * A helper class. This is under org.apache.spark to utilize the private function JsonProtocal.sparkEventFromJson
  * so we can replay a spark event log from file system.
  *
  * @author rxue
  * @since 6/16/16.
  */
object StringToSparkEvent {

  def apply(line: String): SparkListenerEvent = shimIfNeeded(JsonProtocol.sparkEventFromJson(parse(line)))

  def as[T <: SparkListenerEvent](line: String): T = JsonProtocol.sparkEventFromJson(parse(line)).asInstanceOf[T]

  private def shimIfNeeded(event: SparkListenerEvent) = event match {
    case event: SparkListenerLogStart => new SparkListenerLogStartShim(event)
    case default => default
  }
}
