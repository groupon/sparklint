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

import java.net.{BindException, InetAddress}

import com.groupon.sparklint.common.Logging
import org.http4s.MediaType.{`application/json`, `text/html`}
import org.http4s.Response
import org.http4s.dsl._
import org.http4s.headers.`Content-Type`
import org.http4s.server.Server
import org.http4s.server.blaze.BlazeBuilder

import scala.util.{Failure, Success, Try}
import scalaz.concurrent.Task

/**
  * This mix-in trait will start up a server on the port specified
  *
  * Usage: 1. Mount endpoint to RoutingMap.routingMap
  * example: https://github.com/http4s/http4s/blob/master/examples/src/main/scala/com/example/http4s/ExampleService.scala
  * 2. call startServer(port)
  *
  * @author rxue
  * @since 4/25/16.
  */
trait AdhocServer extends RoutingMap with Logging {
  // Random number without special meaning
  var server: Option[Server] = None

  def DEFAULT_PORT: Int

  /** start the server with the routingMap and port
    *
    * @param port port to use
    * @throws IllegalArgumentException if the port is smaller than 0 or greater than 65535
    */
  @throws[IllegalArgumentException]
  def startServer(port: Option[Int] = None): Unit = {
    var portToAttempt = port.getOrElse(DEFAULT_PORT)
    while (server.isEmpty) {
      bindServer(portToAttempt) match {
        case Success(someServer) =>
          server = Some(someServer)
        case Failure(ex: BindException) =>
          val nextPort = portToAttempt + 1
          logDebug(s"Port $portToAttempt has been used, trying $nextPort")
          portToAttempt = nextPort
        case Failure(ex: Throwable) =>
          throw ex
      }
    }
  }

  def bindServer(port: Int): Try[Server] = Try {
    logDebug(s"Starting server on port $port")
    val mountedServer = BlazeBuilder.mountService(router).bindHttp(port, "0.0.0.0").run
    logInfo(s"Server started on $port")
    mountedServer
  }

  def stopServer(): Unit = {
    logInfo("Shutdown request received")
    server.foreach(s => {
      s.shutdown.run
    })
    server = None
    logInfo("Shutdown complete")
  }

  def getServerAddress = server.map(someServer =>
    s"${InetAddress.getLocalHost.getCanonicalHostName}:${someServer.address.getPort}"
  )

  def jsonResponse(textResponse: Task[Response]): Task[Response] = {
    textResponse.withContentType(Some(`Content-Type`(`application/json`)))
  }

  def htmlResponse(textResponse: Task[Response]): Task[Response] = {
    textResponse.withContentType(Some(`Content-Type`(`text/html`)))
  }
}
