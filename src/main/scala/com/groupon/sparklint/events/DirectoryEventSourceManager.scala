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

import java.io.{File, FileFilter}
import java.util.UUID

import com.groupon.sparklint.common.Logging
import org.apache.commons.io.filefilter.FileFileFilter

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Success, Try}

/**
  * @author rxue
  * @since 9/22/16.
  */
case class DirectoryEventSourceManager(dir: File, runImmediately: Boolean, uuid: UUID = UUID.randomUUID()) extends EventSourceManagerLike with Logging {
  require(dir.isDirectory)

  case class PotentialEventSourceDescription(meta: EventSourceMetaLike, file: File)

  private val eventSources: mutable.Map[String, FileEventSource] = TrieMap[String, FileEventSource]()

  val availableEventSources: mutable.Map[String, PotentialEventSourceDescription] = new mutable.LinkedHashMap[String, PotentialEventSourceDescription] with mutable.SynchronizedMap[String, PotentialEventSourceDescription]

  def pull(): Unit = {
    val isFile: FileFilter = FileFileFilter.FILE
    val loadedFiles = eventSources.map(_._2.file.getCanonicalPath).toSet ++ availableEventSources.map(_._2.file.getCanonicalPath)
    val newFiles = dir.listFiles(isFile).flatMap(f => {
      if (loadedFiles.contains(f.getCanonicalPath)) {
        None
      } else {
        Try(EventSourceMeta.fromFile(f)) match {
          case Success(meta) =>
            Some(meta.appIdentifier.toString -> PotentialEventSourceDescription(meta, f))
          case _             => None
        }
      }
    }).toMap
    availableEventSources ++= newFiles
  }

  def pullEventSource(id: String): Try[Unit] = Try {
    if (!eventSources.contains(id)) {
      if (availableEventSources.contains(id)) {
        logInfo(s"Activating event source $id")
        val file = FileEventSource(availableEventSources.remove(id).get.file)
        eventSources += id -> file
        if (runImmediately) {
          Future(file.forwardIfPossible())
        }
      } else {
        throw new NoSuchElementException(s"$id can not be activated in event source manager ${uuid.toString}")
      }
    }
  }

  override def sourceCount: Int = eventSources.size

  override def displayName: String = dir.getName

  override def displayDetails: String = dir.getParentFile.getCanonicalPath

  override def containsEventSource(id: String): Boolean = eventSources.contains(id)

  override def eventSourceDetails: Iterable[EventSourceDetail] = eventSources.map(_._2.getEventSourceDetail)

  override def getSourceDetail(id: String): EventSourceDetail = eventSources(id).getEventSourceDetail

  override def getScrollingSource(id: String): FreeScrollEventSource = eventSources(id)
}
