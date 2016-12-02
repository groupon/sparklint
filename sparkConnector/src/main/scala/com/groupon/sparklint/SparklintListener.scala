package com.groupon.sparklint

import com.groupon.sparklint.events._
import com.groupon.sparklint.ui.UIServer
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.{SparkConf, SparkFirehoseListener}

/**
  * The listener that will be created when provided in --conf spark.extraListeners
  *
  * @author rxue
  * @since 8/18/16.
  */
class SparklintListener(appId: String, appName: String) extends SparkFirehoseListener {

  def this(conf: SparkConf) = {
    this(conf.get("spark.app.id", "AppId"), conf.get("spark.app.name", "AppName"))
  }

  override def onEvent(event: SparkListenerEvent): Unit = {
    // push unconsumed events to the queue so the listener can keep consuming on the main thread
//    buffer.push(event)
  }

  //TODO: support sparkConf based config
  val meta = new EventSourceMeta(appId, appName)
  val progress = new EventProgressTracker()
  val stateManager =  new CompressedStateManager()
  val buffer = BufferedEventSource(appId, Seq(meta, progress, stateManager))

  val detail = SourceAndDetail(buffer, EventSourceDetail(appId, meta, progress, stateManager))
  val eventSourceManager = new EventSourceManager(detail)
  val uiServer = new UIServer(eventSourceManager)

  // ATTN: ordering?
  uiServer.startServer()
  buffer.startConsuming()
}
