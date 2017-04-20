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

import com.groupon.sparklint.ui.{SparklintHomepage, UIEventSourceNavigation}
import org.http4s.MediaType.`text/html`
import org.http4s.dsl._
import org.http4s.headers.`Content-Type`
import org.http4s.{HttpService, Response}

import scalaz.concurrent.Task

/**
  * @author rxue
  * @since 1.0.5
  */
class SparklintFrontend(backend: SparklintBackend) {
  def uiService: HttpService = HttpService {
    case GET -> Root => htmlResponse(Ok(homepage))
    case GET -> Root / "eventSourceManagerList" =>
      htmlResponse(Ok(eventSourceManagerList))
  }

  private def htmlResponse(textResponse: Task[Response]): Task[Response] = {
    textResponse.withContentType(Some(`Content-Type`(`text/html`)))
  }

  private def homepage: String = new SparklintHomepage().HTML.toString

  private def eventSourceManagerList: String = {
    UIEventSourceNavigation(backend).mkString("\n")
  }
}
