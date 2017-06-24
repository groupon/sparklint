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

