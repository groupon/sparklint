/*
 Copyright 2016 Groupon, Inc.
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/
package com.groupon.sparklint.events

import com.groupon.sparklint.common.Utils._
import org.apache.spark.groupon.SparkListenerLogStartShim
import org.apache.spark.scheduler._

import scala.collection.Map
import scala.collection.immutable.HashMap

/**
  * The EventSourceMeta receiver routes the early events in the event log in order to extract information
  * about the application itself.
  *
  * @author swhitear 
  * @since 9/29/16.
  */
class EventSourceMeta extends EventSourceMetaLike with EventReceiverLike {

  type EnvironmentData = Map[String, Seq[(String, String)]]

  var appIdOpt: Option[String] = None

  def appId: String = appIdOpt.getOrElse(UNKNOWN_STRING)

  var appNameOpt: Option[String] = None

  def appName: String = appNameOpt.getOrElse(UNKNOWN_STRING)

  var userOpt: Option[String] = None

  def user: String = userOpt.getOrElse(UNKNOWN_STRING)

  var versionOpt: Option[String] = None

  def version: String = versionOpt.getOrElse(UNKNOWN_STRING)

  var hostOpt: Option[String] = None

  def host: String = hostOpt.getOrElse(UNKNOWN_STRING)

  var portOpt: Option[Int] = None

  def port: Int = portOpt.getOrElse(UNKNOWN_NUMBER.toInt)

  var maxMemoryOpt: Option[Long] = None

  def maxMemory: Long = maxMemoryOpt.getOrElse(UNKNOWN_NUMBER)

  var startTimeOpt: Option[Long] = None

  def startTime: Long = startTimeOpt.getOrElse(UNKNOWN_NUMBER)

  var endTimeOpt: Option[Long] = None

  def endTime: Long = endTimeOpt.getOrElse(UNKNOWN_NUMBER)

  var environmentOpt: Option[EnvironmentData] = None

  def environment: EnvironmentData = environmentOpt.getOrElse(HashMap.empty)

  def fullName = s"$appName ($appId)"


  override protected def preprocLogStart(event: SparkListenerLogStartShim) = setVersionState(event)

  override protected def preprocAddBlockManager(event: SparkListenerBlockManagerAdded) = setBlockManagerState(event)

  override protected def preprocEnvironmentUpdate(event: SparkListenerEnvironmentUpdate) = setEnvironmentState(event)

  override protected def preprocAddApp(event: SparkListenerApplicationStart) = setAppStartState(event)

  override protected def preprocEndApp(event: SparkListenerApplicationEnd) = setAppEndState(event)


  private def setVersionState(event: SparkListenerLogStartShim) = {
    versionOpt = Some(event.sparkVersion)
  }

  private def setBlockManagerState(event: SparkListenerBlockManagerAdded) = {
    hostOpt = Some(event.blockManagerId.host)
    portOpt = Some(event.blockManagerId.port)
    maxMemoryOpt = Some(event.maxMem)
  }

  private def setEnvironmentState(event: SparkListenerEnvironmentUpdate) = {
    environmentOpt = Some(event.environmentDetails)
  }

  private def setAppStartState(event: SparkListenerApplicationStart) = {
    appIdOpt = event.appId
    appNameOpt = Some(event.appName)
    userOpt = Some(event.sparkUser)
    startTimeOpt = Some(event.time)
    println(s"Set start time to ${event.time}")
  }

  private def setAppEndState(event: SparkListenerApplicationEnd) = {
    endTimeOpt = Some(event.time)
    println(s"Set end time to ${event.time}")
  }

  override def toString: String = s"AppId: $appId, Name: $appName"
}
