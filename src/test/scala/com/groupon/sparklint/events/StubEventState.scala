package com.groupon.sparklint.events

import com.groupon.sparklint.data.SparklintStateLike
import com.groupon.sparklint.data.compressed.CompressedState
import org.apache.spark.scheduler.SparkListenerEvent

import scala.collection.mutable.ArrayBuffer

/**
  *
  * @author swhitear 
  * @since 11/16/16.
  */
case class StubEventState(onEvents: ArrayBuffer[SparkListenerEvent] = ArrayBuffer.empty,
                          unEvents: ArrayBuffer[SparkListenerEvent] = ArrayBuffer.empty,
                          state: SparklintStateLike = CompressedState.empty)
  extends EventStateLike {

  override def onEvent(event: SparkListenerEvent): Unit = onEvents += event

  override def unEvent(event: SparkListenerEvent): Unit = unEvents += event

  override def getState: SparklintStateLike = state
}
