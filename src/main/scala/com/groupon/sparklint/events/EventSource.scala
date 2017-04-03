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

import java.io.{File, FileInputStream}
import java.util.UUID
import java.util.zip.ZipInputStream

import com.groupon.sparklint.data.SparklintStateLike
import org.apache.commons.io.{FilenameUtils, IOUtils}
import org.apache.spark.SparkConf
import org.apache.spark.groupon.{SparkListenerLogStartShim, StringToSparkEvent}
import org.apache.spark.io.{LZ4CompressionCodec, LZFCompressionCodec, SnappyCompressionCodec}
import org.apache.spark.scheduler.SparkListenerApplicationStart

import scala.collection.JavaConverters._

/**
  * @author rxue
  * @since 1.0.5
  */
trait EventSource {
  val uuid: UUID

  val appMeta: EventSourceMeta
  val progressTracker: EventProgressTracker

  def appState: SparklintStateLike

  def hasNext: Boolean

  def forward(): Boolean

  def hasPrevious: Boolean = false
}

object EventSource {
  def fromFile(file: File, compressStorage: Boolean = false): FreeScrollEventSource = {
    fromStringIterator(IOUtils.lineIterator(new FileInputStream(file), null: String).asScala, file.getName, compressStorage)
  }

  /**
    * @throws UnrecognizedLogFileException if the file cannot be parsed
    * @return
    */
  def fromStringIterator(stringIterator: Iterator[String], sourceName: String, compressStorage: Boolean, uuid: UUID = UUID.randomUUID()): FreeScrollEventSource = {
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
    val appMeta = EventSourceMeta(appStartEvent.appId, appStartEvent.appAttemptId, appStartEvent.appName, Some(sparkVersion), appStartEvent.time)
    val eventIterator = stringIterator.flatMap(StringToSparkEvent.apply)
    new IteratorEventSource(uuid, appMeta, eventIterator, compressStorage)
  }

  def fromZipStream(zis: ZipInputStream, esUuid: String, sourceName: String, compressStorage: Boolean = false): FreeScrollEventSource = {
    if (zis.available() > 0) {
      val entry = zis.getNextEntry
      if (entry == null) {
        throw UnrecognizedLogFileException(sourceName, Some(s"no zip entry available"))
      }
      val decompressedStream = FilenameUtils.getExtension(entry.getName) match {
        case "" => zis
        case "lz4" => new LZ4CompressionCodec(new SparkConf()).compressedInputStream(zis)
        case "lzf" => new LZFCompressionCodec(new SparkConf()).compressedInputStream(zis)
        case "snappy" => new SnappyCompressionCodec(new SparkConf()).compressedInputStream(zis)
        case ext => throw UnrecognizedLogFileException(sourceName, Some(s"no depressing codec available for file extension '$ext'"))
      }
      fromStringIterator(IOUtils.toString(decompressedStream, null: String).split("\n").iterator, sourceName, compressStorage, UUID.fromString(esUuid))
    } else {
      throw UnrecognizedLogFileException(sourceName, Some(s"no zip entry available"))
    }
  }
}
