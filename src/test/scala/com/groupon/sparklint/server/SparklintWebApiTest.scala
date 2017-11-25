package com.groupon.sparklint.server

import org.scalatest.FlatSpec

/**
  * @author rxue
  * @since 11/23/17.
  */
class SparklintWebApiTest extends FlatSpec {
  it should "run" in {
    object TestServer extends AdhocServer with SparklintWebApi {
      override def DEFAULT_PORT = 4040
    }

    TestServer.startServer()
    scala.io.StdIn.readLine()
  }
}
