package com.groupon.sparklint.events

import java.util.concurrent.{BlockingQueue, LinkedBlockingDeque}

import org.apache.spark.scheduler.SparkListenerEvent

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * An EventSource that uses an intermediary queue to buffer live events before updating receivers.
  *
  * @author swhitear
  * @since 8/18/16.
  */
case class BufferedEventSource(eventSourceId: String, receivers: Seq[EventReceiverLike])
  extends EventSourceLike {

  val buffer: BlockingQueue[SparkListenerEvent] = new LinkedBlockingDeque()

  def push(event: SparkListenerEvent): Unit = {
    buffer.add(event)
  }

  def startConsuming() = Future {
    while (true) {
      val event = buffer.take()
      receivers.foreach(r => {
        r.preprocess(event)
        r.onEvent(event)
      })
    }
    // TODO make cancelable
  }
}
