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

package com.groupon.sparklint.ui.views

import com.groupon.sparklint.events._

import scala.xml.Node

/**
  * @author rxue
  * @since 2/13/17.
  */
object UIEventSourceNavigation {
  /**
    * Render a html fragment to list all [[EventSourceManagerLike]] under the [[RootEventSourceManager]]
    *
    * @param root the [[RootEventSourceManager]]
    * @return a list of li tags
    */
  def apply(root: RootEventSourceManager): Seq[Node] = {
    for (esm <- root.eventSourceManagers) yield
      <li class="eventSourceManager" data-esm-uuid={esm.uuid.toString}>
        <a href="#" class="has-arrow">
          <i class={iconForEventSourceManager(esm)}></i>{esm.displayName}<span class="fa arrow"></span>
        </a>
        {esm match {
          case desm: DirectoryEventSourceManager      => listDirectorySourceManager(desm)
          case sfesm: SingleFileEventSourceManager    => listSingleFileEventSourceManager(sfesm)
          case imesm: InMemoryEventSourceManager      => listInMemoryEventSourceManager(imesm)
          case hsesm: HistoryServerEventSourceManager => listHistoryServerEventSourceManager(hsesm)
        }}
      </li>
  }

  def iconForEventSourceManager(esm: EventSourceManagerLike): String = esm match {
    case _: DirectoryEventSourceManager     => "fa fa-folder-open fa-fw"
    case _: InMemoryEventSourceManager      => "fa fa-download fa-fw"
    case _: HistoryServerEventSourceManager => "fa fa-database fa-fw"
    case _: SingleFileEventSourceManager    => "fa fa-file-text fa-fw"
    case _                                  => "fa fa-sitemap fa-fw"
  }

  def listDirectorySourceManager(esm: DirectoryEventSourceManager): Seq[Node] =
    <ul class="nav nav-second-level collapse in" aria-expanded="false">
    {
      for (source <- esm.eventSourceDetails)
        yield eventSourceListItem(source.meta.appIdentifier.toString, source.meta, source.progress)
    }
    {
      for ((id, source) <- esm.availableEventSources) yield {
        <li data-value={id}>
          <a href="#" class="inactiveApp" data-value={id}>
            <div><i class="fa fa-file fa-tw"></i> {source.meta.appName}</div>
            <div><small>{id}</small></div>
          </a>
        </li>
      }
    }
    </ul>

  def listSingleFileEventSourceManager(esm: SingleFileEventSourceManager): Seq[Node] =
    <ul class="nav nav-second-level collapse in" aria-expanded="true">
    {
      val source = esm.fileEventSource
      eventSourceListItem(source.meta.appIdentifier.toString, source.meta, source.progress)
    }
    </ul>

  def listInMemoryEventSourceManager(esm: InMemoryEventSourceManager): Seq[Node] =
    <ul class="nav nav-second-level collapse in" aria-expanded="true">
    {
      val source = esm.inMemoryEventSource
      eventSourceListItem(source.meta.appIdentifier.toString, source.meta, source.progress)
    }
      <li class="divider"></li>
    </ul>

  def listHistoryServerEventSourceManager(esm: HistoryServerEventSourceManager): Seq[Node] =
    <ul class="nav nav-second-level collapse in" aria-expanded="false">
    {
      for (source <- esm.eventSourceDetails) yield {
        eventSourceListItem(source.meta.appIdentifier.toString, source.meta, source.progress)
      }
    }
    {
      for (meta <- esm.availableEventSources) yield {
        <li data-value={meta.appIdentifier.toString}>
          <a href="#" class="inactiveApp" data-value={meta.appIdentifier.toString}>
            <div><i class="fa fa-file fa-tw"></i> {meta.appName}</div>
            <div><small>{meta.appIdentifier.toString}</small></div>
          </a>
        </li>
      }
    }
    </ul>

  def eventSourceListItem(esId: String, meta: EventSourceMetaLike, progress: EventProgressTrackerLike): Seq[Node] =
    <li data-value={esId}>
      <a href="#" class="sparklintApp" data-value={esId}>
        <strong>App:</strong>{meta.appName}<p class="text-center" id={uniqueId(esId, "app-prog")}>
        {progress.eventProgress.description}
      </p>
        <div class="progress active">
          <div class="progress-bar" role="progressbar" id={uniqueId(esId, "progress-bar")}
               aria-valuenow={progress.eventProgress.percent.toString} aria-valuemin="0" aria-valuemax="100"
               style={widthStyle(progress.eventProgress)}>
          </div>
        </div>
      </a>
    </li>

  private def widthStyle(esp: EventProgress) = s"width: ${esp.percent}%"

  private def uniqueId(appId: String, idType: String) = s"$appId-$idType"
}
