package com.groupon.sparklint.events

import com.groupon.sparklint.common.Utils

/**
  * An extension of EventSourceReceiverLike that collects metadata about the underlying EventSource.
  *
  * @author swhitear 
  * @since 11/21/16.
  */
trait EventSourceMetaLike {
  private val STANDARD_APP_PREFIX = "application_"

  lazy val trimmedId = appId.replace(STANDARD_APP_PREFIX, "")

  def version: String

  def host: String

  def port: Int

  def maxMemory: Long

  def appId: String

  def appName: String

  def user: String

  def startTime: Long

  def endTime: Long

  def fullName: String

  def nameOrId: String = if (missingName) appId else appName

  private def missingName = appName.isEmpty || appName == Utils.UNKNOWN_STRING

}
