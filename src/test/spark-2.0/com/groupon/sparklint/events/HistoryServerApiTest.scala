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

import java.util.Date

import org.http4s.Uri._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * @author rxue
  * @since 6/24/17.
  */
class HistoryServerApiTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  lazy val mockServer = new MockHistoryServer(4242)

  it should "parse applications correctly" in {
    val api = HistoryServerApi("test", uri("http://localhost:4242"))
    val applications = api.getApplications(new Date(1422921600000L))
    applications.length shouldBe 6
    applications.head shouldBe ApplicationHistoryInfo("local-1430917381534", "Spark shell", List(
      ApplicationAttemptInfo(None, new Date(1430917380893L), new Date(1430917391398L), "irashid", completed = true)))
    applications(1) shouldBe ApplicationHistoryInfo("local-1430917381535", "Spark shell", List(
      ApplicationAttemptInfo(Some("2"), new Date(1430917380893L), new Date(1430917380950L), "irashid", completed = true),
      ApplicationAttemptInfo(Some("1"), new Date(1430917380880L), new Date(1430917380890L), "irashid", completed = true)))
    applications(2) shouldBe ApplicationHistoryInfo("local-1426533911241", "Spark shell", List(
      ApplicationAttemptInfo(Some("2"), new Date(1426633910242L), new Date(1426633945177L), "irashid", completed = true),
      ApplicationAttemptInfo(Some("1"), new Date(1426533910242L), new Date(1426533945177L), "irashid", completed = true)))
      applications(3) shouldBe ApplicationHistoryInfo("local-1425081759269", "Spark shell", List(
        ApplicationAttemptInfo(None, new Date(1425081758277L), new Date(1425081766912L), "irashid", completed = true)))
      applications(4) shouldBe ApplicationHistoryInfo("local-1422981780767", "Spark shell", List(
        ApplicationAttemptInfo(None, new Date(1422981779720L), new Date(1422981788731L), "irashid", completed = true)))
      applications(5) shouldBe ApplicationHistoryInfo("local-1422981759269", "Spark shell", List(
        ApplicationAttemptInfo(None, new Date(1422981758277L), new Date(1422981766912L), "irashid", completed = true)))
  }

  override protected def beforeAll(): Unit = {
    mockServer.startServer()
  }

  override protected def afterAll(): Unit = {
    mockServer.stopServer()
  }
}
