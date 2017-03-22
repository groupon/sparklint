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

package com.groupon.sparklint.events

import java.io.File

import com.groupon.sparklint.common.Utils._
import org.apache.spark.groupon.{SparkListenerLogStartShim, StringToSparkEvent}
import org.apache.spark.scheduler._

import scala.collection.Map
import scala.collection.immutable.HashMap
import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
  * The EventSourceMeta receiver routes the early events in the event log in order to extract information
  * about the application itself.
  *
  * @author swhitear
  * @since 9/29/16.
  */
class EventSourceMeta(identifier: EventSourceIdentifier, override val appName: String) extends EventSourceMetaLike with EventReceiverLike {

  type EnvironmentData = Map[String, Seq[(String, String)]]

  def appIdentifier: EventSourceIdentifier = identifier

  private var userOpt: Option[String] = None

  def user: String = userOpt.getOrElse(UNKNOWN_STRING)

  private var versionOpt: Option[String] = None

  def version: String = versionOpt.getOrElse(UNKNOWN_STRING)

  private var hostOpt: Option[String] = None

  def host: String = hostOpt.getOrElse(UNKNOWN_STRING)

  private var portOpt: Option[Int] = None

  def port: Int = portOpt.getOrElse(UNKNOWN_NUMBER.toInt)

  private var maxMemoryOpt: Option[Long] = None

  def maxMemory: Long = maxMemoryOpt.getOrElse(UNKNOWN_NUMBER)

  private var startTimeOpt: Option[Long] = None

  def startTime: Long = startTimeOpt.getOrElse(UNKNOWN_NUMBER)

  private var endTimeOpt: Option[Long] = None

  def endTime: Long = endTimeOpt.getOrElse(UNKNOWN_NUMBER)

  private var environmentOpt: Option[EnvironmentData] = None

  def environment: EnvironmentData = environmentOpt.getOrElse(HashMap.empty)

  def fullName = s"$appName ($appIdentifier)"


  override protected def preprocLogStart(event: SparkListenerLogStartShim): Unit = {
    versionOpt = Some(event.sparkVersion)
  }

  override protected def preprocAddBlockManager(event: SparkListenerBlockManagerAdded): Unit = {
    hostOpt = Some(event.blockManagerId.host)
    portOpt = Some(event.blockManagerId.port)
    maxMemoryOpt = Some(event.maxMem)
  }

  override protected def preprocEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit = {
    environmentOpt = Some(event.environmentDetails)
  }

  override protected def preprocAddApp(event: SparkListenerApplicationStart): Unit = {
    userOpt = Some(event.sparkUser)
    startTimeOpt = Some(event.time)
  }

  override protected def preprocEndApp(event: SparkListenerApplicationEnd): Unit = {
    endTimeOpt = Some(event.time)
  }

  override def toString: String = s"AppId: $appIdentifier, Name: $appName"
}

object EventSourceMeta {
  def fromFile(file: File): EventSourceMeta = {
    Try(Source.fromFile(file)) match {
      case Success(sparkEventLog) =>
        sparkEventLog.getLines().map(StringToSparkEvent.apply).collectFirst({
          case e: SparkListenerApplicationStart =>
            new EventSourceMeta(EventSourceIdentifier(e.appId.getOrElse(file.getName), e.appAttemptId), e.appName)
        }) match {
          case Some(meta) => meta
          case None       => throw new NoSuchElementException(s"Cannot find SparkListenerApplicationStart event from ${file.getName}.")
        }
      case Failure(ex)            =>
        throw ex
    }
  }
}
