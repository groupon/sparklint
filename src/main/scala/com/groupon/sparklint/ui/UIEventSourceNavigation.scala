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

package com.groupon.sparklint.ui

import com.groupon.sparklint.SparklintBackend
import com.groupon.sparklint.events._

import scala.xml.Node

/**
  * @author rxue
  * @since 2/13/17.
  */
object UIEventSourceNavigation {
  /**
    * Render a html fragment to list all [[EventSourceGroupManager]] under the [[SparklintBackend]]
    *
    * @param backend the [[SparklintBackend]]
    * @return a list of li tags
    */
  def apply(backend: SparklintBackend): Seq[Node] = {
    for (esm <- backend.listEventSourceGroupManagers) yield
      <li class="eventSourceManager" data-esm-uuid={esm.uuid.toString}>
        <a href="#" class="has-arrow">
          <i class={iconForEventSourceManager(esm)}></i>{esm.name}<span class="fa arrow"></span>
        </a>{esm match {
        case hsesgm: HistoryServerEventSourceGroupManager => listHistoryServerEventSourceGroupManager(hsesgm)
        case gesgm: GenericEventSourceGroupManager => listGenericEventSourceGroupManager(gesgm)
      }}
      </li>
  }

  def iconForEventSourceManager(esm: EventSourceGroupManager): String = esm match {
    case _: FolderEventSourceGroupManager => "fa fa-folder-open fa-fw"
    case _: HistoryServerEventSourceGroupManager => "fa fa-database fa-fw"
    case _: GenericEventSourceGroupManager => "fa fa-file-text fa-fw"
    case _ => "fa fa-sitemap fa-fw"
  }

  def listGenericEventSourceGroupManager(esm: GenericEventSourceGroupManager): Seq[Node] =
    <ul class="nav nav-second-level collapse in" aria-expanded="false">
      {for (source <- esm.eventSources)
      yield eventSourceListItem(source.uuid.toString, source.appMeta, source.progressTracker)}
    </ul>

  def listHistoryServerEventSourceGroupManager(esm: HistoryServerEventSourceGroupManager): Seq[Node] =
    <ul class="nav nav-second-level collapse in" aria-expanded="false">
      {for (source <- esm.eventSources)
      yield eventSourceListItem(source.uuid.toString, source.appMeta, source.progressTracker)}{for ((esUuid, meta) <- esm.availableSources) yield {
      <li data-value={esUuid}>
        <a href="#" class="inactiveApp" data-value={esUuid}>
          <div>
            <i class="fa fa-file fa-tw"></i>{meta.appName}
          </div>
          <div>
            <small>
              {meta.fullAppId}
            </small>
          </div>
        </a>
      </li>
    }}
    </ul>

  def eventSourceListItem(esUuid: String, meta: EventSourceMeta, progress: EventProgressTrackerLike): Seq[Node] = {
    <li data-value={esUuid}>
      <a href="#" class="sparklintApp" data-value={esUuid}>
        <strong>App:</strong>{meta.appName}<p class="text-center" id={uniqueId(esUuid, "app-prog")}>
        {progress.eventProgress.description}
      </p>
        <div class="progress active">
          <div class="progress-bar" role="progressbar" id={uniqueId(esUuid, "progress-bar")}
               aria-valuenow={progress.eventProgress.percent.toString} aria-valuemin="0" aria-valuemax="100"
               style={widthStyle(progress.eventProgress)}>
          </div>
        </div>
      </a>
    </li>
  }

  private def widthStyle(esp: EventProgress) = s"width: ${esp.percent}%"

  private def uniqueId(appId: String, idType: String) = s"$appId-$idType"
}
