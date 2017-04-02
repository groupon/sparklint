package com.groupon.sparklint

import com.groupon.sparklint.common.SparklintConfig
import com.groupon.sparklint.server.AdhocServer

/**
  * The class that contains the backend and ui
  *
  * @author rxue
  * @since 1.0.5
  */
class Sparklint(config: SparklintConfig) extends AdhocServer {
  val backend = new SparklintBackend()
  val frontend = new SparklintFrontend()
  registerService("", frontend.uiService)
  registerService("backend", backend.backendService)

  override def DEFAULT_PORT: Int = config.defaultPort

}
