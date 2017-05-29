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
import com.groupon.sparklint.events.{GenericEventSourceGroupManager, ListenerEventSource}
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.{SparkConf, SparkFirehoseListener}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * The listener that will be created when provided in --conf spark.extraListeners
  *
  * @author rxue
  * @since 8/18/16.
  */
class SparklintListener(appId: String, appName: String, config: SparklintConfig) extends SparkFirehoseListener {

  val sparklint = new Sparklint(config)
  val esgm = new GenericEventSourceGroupManager("SparklintListener", closeable = false)
  val liveEventSource = new ListenerEventSource(appId, appName)

  def this(conf: SparkConf) = {
    this(conf.get("spark.app.id", "AppId"), conf.get("spark.app.name", "AppName"), new SparkConfSparklintConfig(conf))
  }

  override def onEvent(event: SparkListenerEvent): Unit = {
    liveEventSource.onEvent(event)
  }

  esgm.registerEventSource(liveEventSource)
  sparklint.backend.append(esgm)
  Future(liveEventSource.start())
  sparklint.startServer()
}
