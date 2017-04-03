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

package com.groupon.sparklint.server

import org.http4s.HttpService
import org.http4s.client.Client
import org.http4s.client.blaze.PooledHttp1Client
import org.http4s.dsl._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * @author rxue
  * @since 4/25/16.
  */
class AdhocServerTest extends FlatSpec with BeforeAndAfterAll with Matchers {

  var client: Client = _

  def getService = new AdhocServer with HeartBeatService {

    override def DEFAULT_PORT: Int = 40000

    val secret = 4242

    registerService("", HttpService {
      case GET -> Root =>
        Ok(secret.toString)
      case GET -> Root / "secret" / yourSecret =>
        Ok((secret + yourSecret.toInt).toString)
    })
  }

  it should "run" in {
    val service = getService
    try {
      service.startServer(None)
      val server = service.server.get
      val heartbeatCheck = client.getAs[String](s"http://localhost:${server.address.getPort}/heartbeat")
      heartbeatCheck.run shouldBe "OK"
      val homepageCheck = client.getAs[String](s"http://localhost:${server.address.getPort}/")
      homepageCheck.run shouldBe "4242"
      val secretCheck = client.getAs[String](s"http://localhost:${server.address.getPort}/secret/2424")
      secretCheck.run shouldBe "6666"
    } finally {
      service.stopServer()
    }
  }

  it should "run on default port" in {
    val service = getService
    try {
      service.startServer(None)
      service.server.get.address.getPort shouldBe service.DEFAULT_PORT
    } finally {
      service.stopServer()
    }
  }

  it should "run on specified port" in {
    val service = getService
    try {
      service.startServer(Some(2345))
      service.server.get.address.getPort shouldBe 2345
    } finally {
      service.stopServer()
    }
  }

  it should "throw up on illegal port" in {
    val service = getService
    try {
      intercept[IllegalArgumentException] {
        service.startServer(Some(234555))
      }
    } finally {
      service.stopServer()
    }
  }

  it should "run if port is occupied" in {
    val service = getService
    val otherServer = service.bindServer(2345)
    try {
      service.startServer(Some(2345))
      service.server.get.address.getPort shouldBe 2346
    } finally {
      service.stopServer()
      otherServer.get.shutdownNow()
    }
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    client = PooledHttp1Client()
  }
}
