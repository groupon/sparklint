package com.groupon.sparklint

import org.http4s.MediaType.`text/html`
import org.http4s.dsl._
import org.http4s.headers.`Content-Type`
import org.http4s.{HttpService, Response}

import scalaz.concurrent.Task

/**
  * @author rxue
  * @since 1.0.5
  */
class SparklintFrontend {
  private def htmlResponse(textResponse: Task[Response]): Task[Response] = {
    textResponse.withContentType(Some(`Content-Type`(`text/html`)))
  }

  def uiService: HttpService = HttpService {
    case GET -> Root => htmlResponse(Ok(homepage))
  }

  private def homepage: String = ???
}
