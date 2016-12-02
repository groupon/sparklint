package com.groupon.sparklint.common

import org.slf4j.{Logger, LoggerFactory}

/**
  * A trait for enabling logging. This is pretty much similar to what Spark does internally
  * We replicate it here since the Spark one is private
  *
  * @author rxue
  * @since 7/4/16.
  */
trait Logging {
  @transient private var _logger: Logger = null

  protected def log: Logger = {
    if (_logger == null) {
      val logName = this.getClass.getName.stripSuffix("$")
      _logger = LoggerFactory.getLogger(logName)
    }
    _logger
  }

  def logError(message: String): Unit = {
    log.error(message)
  }

  def logError(message: String, ex: Throwable): Unit = {
    log.error(message, ex)
  }

  def logWarn(message: String): Unit = {
    log.warn(message)
  }

  def logInfo(message: String): Unit = {
    log.info(message)
  }

  def logDebug(message: String): Unit = {
    log.debug(message)
  }

  def logTrace(message: String): Unit = {
    log.trace(message)
  }
}
