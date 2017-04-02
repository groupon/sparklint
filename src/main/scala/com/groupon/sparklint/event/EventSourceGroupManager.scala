package com.groupon.sparklint.event

import java.io.{File, FileNotFoundException}

/**
  * Created by Roboxue on 2017/4/2.
  */
trait EventSourceGroupManager {
  /**
    * @return The display name of this manager
    */
  def name: String

  /**
    * @return if this manager can be closed by user
    */
  def closeable: Boolean

  /**
    * Register an event source
    *
    * @param es the [[EventSource]] to register
    * @return true if success, false if the event source has been registered already
    */
  def registerEventSource(es: EventSource): Boolean = ???

}

/**
  * @throws FileNotFoundException if the folder provided doesn't exist or is a file
  * @param folder the folder of the log files
  */
@throws[FileNotFoundException]
class FolderEventSourceGroupManager(folder: File) extends EventSourceGroupManager {
  if (!folder.exists() || folder.isFile) {
    throw new FileNotFoundException(folder.getAbsolutePath)
  }

  override def name: String = folder.getName

  override def closeable: Boolean = true
}

case class GenericEventSourceGroupManager(name: String, closeable: Boolean) extends EventSourceGroupManager
