package com.groupon.sparklint

import java.io.File

import com.groupon.sparklint.common.Logging
import com.groupon.sparklint.event.{EventSourceGroupManager, FolderEventSourceGroupManager}
import org.http4s.HttpService
import org.http4s.dsl._

import scala.collection.mutable.ListBuffer

/**
  * The backend api
  *
  * @author rxue
  * @since 1.0.5
  */
class SparklintBackend
  extends Logging {
  private val esgManagers = ListBuffer.empty[EventSourceGroupManager]

  /**
    * @return All available [[EventSourceGroupManager]]
    */
  def listEventSourceGroupManagers: Seq[EventSourceGroupManager] = esgManagers

  /** Appends the given [[EventSourceGroupManager]]
    *
    * @param elems the [[EventSourceGroupManager]] to append.
    */
  def append(elems: EventSourceGroupManager*): Unit = esgManagers.append(elems: _*)

  def backendService: HttpService = ???

  protected def appendFolderManager(folder: File): Unit = {
    append(new FolderEventSourceGroupManager(folder))
  }

}
