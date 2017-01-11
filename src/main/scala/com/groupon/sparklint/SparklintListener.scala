/*
 * Copyright 2016 Groupon, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.groupon.sparklint

import com.groupon.sparklint.common.{SparkConfSparklintConfig, SparklintConfig}
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
class SparklintListener(appId: String, appName: String, config: SparklintConfig) extends SparkFirehoseListener {

  def this(conf: SparkConf) = {
    this(conf.get("spark.app.id", "AppId"), conf.get("spark.app.name", "AppName"), new SparkConfSparklintConfig(conf))
  }

  override def onEvent(event: SparkListenerEvent): Unit = {
    // push unconsumed events to the queue so the listener can keep consuming on the main thread
    buffer.push(event)
  }

  //TODO: support sparkConf based config
  val meta = new EventSourceMeta(appId, appName)
  val progress = new EventProgressTracker()
  val stateManager =  new CompressedStateManager()
  val buffer = DeferredEventSource(appId, Seq(meta, progress, stateManager))

  val detail = SourceAndDetail(buffer, EventSourceDetail(appId, meta, progress, stateManager))
  val eventSourceManager = new EventSourceManager(detail)
  val uiServer = new UIServer(eventSourceManager, config)

  // ATTN: ordering?
  uiServer.startServer()
  buffer.startConsuming()
}
