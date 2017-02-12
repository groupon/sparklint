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

import org.apache.commons.io.filefilter.FileFileFilter

import scala.collection.mutable
import scala.util.{Success, Try}

/**
  * @author rxue
  * @since 9/22/16.
  */
case class DirectoryEventSourceManager(dir: File) extends EventSourceManagerLike {
  require(dir.isDirectory)

  case class PotentialEventSourceDescription(file: File)

  private val eventSources: mutable.Map[EventSourceIdentifier, FileEventSource] = new mutable.LinkedHashMap[EventSourceIdentifier, FileEventSource] with mutable.SynchronizedMap[EventSourceIdentifier, FileEventSource]

  val availableEventSources: mutable.Map[EventSourceMeta, PotentialEventSourceDescription] = new mutable.LinkedHashMap[EventSourceMeta, PotentialEventSourceDescription] with mutable.SynchronizedMap[EventSourceMeta, PotentialEventSourceDescription]

  def pull(): Unit = {
    val isFile: FileFilter = FileFileFilter.FILE
    val loadedFiles = eventSources.map(_._2.file.getCanonicalPath).toSet ++ availableEventSources.map(_._2.file.getCanonicalPath)
    val newFiles = dir.listFiles(isFile).flatMap(f => {
      if (loadedFiles.contains(f.getCanonicalPath)) {
        None
      } else {
        Try(EventSourceMeta.fromFile(f)) match {
          case Success(meta) =>
            Some(meta -> PotentialEventSourceDescription(f))
          case _             => None
        }
      }
    }).toMap
    availableEventSources ++= newFiles
  }

  def pullEventSource(meta: EventSourceMeta): Try[Unit] = Try {
    if (!eventSources.contains(meta.appIdentifier)) {
      eventSources += meta.appIdentifier -> FileEventSource(availableEventSources.remove(meta).get.file)
    }
  }

  override def sourceCount: Int = eventSources.size

  override def displayName: String = dir.getName

  override def displayDetails: String = dir.getParentFile.getCanonicalPath

  override def containsEventSource(id: EventSourceIdentifier): Boolean = eventSources.contains(id)

  override def eventSourceDetails: Iterable[EventSourceDetail] = eventSources.map(_._2.getEventSourceDetail)

  override def getSourceDetail(id: EventSourceIdentifier): EventSourceDetail = eventSources(id).getEventSourceDetail

  override def getScrollingSource(id: EventSourceIdentifier): FreeScrollEventSource = eventSources(id)
}
