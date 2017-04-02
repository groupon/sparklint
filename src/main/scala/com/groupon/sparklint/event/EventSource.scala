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

package com.groupon.sparklint.event

import java.io.{File, InputStream}

import org.apache.spark.groupon.{SparkListenerLogStartShim, StringToSparkEvent}
import org.apache.spark.scheduler.{SparkListenerApplicationStart, SparkListenerEnvironmentUpdate}

import scala.io.Source

/**
  * @author rxue
  * @since 1.0.5
  */
trait EventSource {
  val appMeta: SparkAppMeta
}

object EventSource {
  def fromFile(file: File): EventSource = {
    fromStringIterator(Source.fromFile(file).getLines(), file.getName)
  }

  def fromStringIterator(stringIterator: Iterator[String], sourceName: String): EventSource = {
    if (!stringIterator.hasNext) {
      throw UnrecognizedLogFileException(sourceName, Some(s"content is empty"))
    }
    var lineBuffer = stringIterator.next()
    val sparkVersion = StringToSparkEvent(lineBuffer) match {
      case Some(sparkLogStart: SparkListenerLogStartShim) =>
        sparkLogStart.sparkVersion
      case _ =>
        throw UnrecognizedLogFileException(sourceName, Some(s"firstLine '$lineBuffer' is not a SparkListenerLogStart event"))
    }
    do {
      if (stringIterator.hasNext) {
        lineBuffer = stringIterator.next()
      } else {
        throw UnrecognizedLogFileException(sourceName, Some(s"doesn't contain a SparkListenerApplicationStart event"))
      }
    } while (!StringToSparkEvent(lineBuffer).exists(_.isInstanceOf[SparkListenerApplicationStart]))
    val appStartEvent = StringToSparkEvent(lineBuffer).get.asInstanceOf[SparkListenerApplicationStart]
    val appMeta = SparkAppMeta(appStartEvent.appId, appStartEvent.appAttemptId, appStartEvent.appName, Some(sparkVersion))
    val eventIterator = stringIterator.flatMap(StringToSparkEvent.apply)
    new IteratorEventSource(appMeta, eventIterator)
  }
}
