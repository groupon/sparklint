package com.groupon.sparklint.events

import org.apache.spark.groupon.StringToSparkEvent
import org.apache.spark.scheduler.SparkListenerEvent

import scala.io.Source

/**
  * Like [[FileEventSource]] but populates the buffer via HTTP from Spark History.
  *
  * @author superbobry
  * @since 1/11/17.
  */
case class HistoryEventSource(
    override val eventSourceId: String,
    contents: Array[Char],
    receivers: Seq[EventReceiverLike]
) extends BufferedEventSource {

  override def fillBuffer(): IndexedSeq[SparkListenerEvent] = {
    Source.fromChars(contents).getLines().map { line =>
      val event = StringToSparkEvent(line)
      preprocessEvent(event)
      event
    }.toIndexedSeq
  }

  private def preprocessEvent(event: SparkListenerEvent) = receivers.foreach(_.preprocess(event))

  override def onEvent(event: SparkListenerEvent): Unit = receivers.foreach(_.onEvent(event))

  override def unEvent(event: SparkListenerEvent): Unit = receivers.foreach(_.unEvent(event))
}
