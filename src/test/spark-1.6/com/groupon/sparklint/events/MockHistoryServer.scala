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

import com.groupon.sparklint.server.AdhocServer
import org.apache.commons.io.IOUtils
import org.http4s.HttpService
import org.http4s.dsl._

/**
  * @author rxue
  * @since 6/24/17.
  */
class MockHistoryServer(port: Int) extends AdhocServer {
  override def DEFAULT_PORT: Int = port

  val classLoader: ClassLoader = getClass.getClassLoader
  val mockService = HttpService {
    case GET -> Root / "api" / "v1" / "applications" =>
      import scala.collection.JavaConversions._
      val context: Seq[String] = IOUtils.readLines(classLoader.getResourceAsStream("history_source/application_list_json_expectation/spark-1.6.0.json"))
      jsonResponse(Ok(context.mkString(System.lineSeparator())))
  }
  registerService("", mockService)
}

