package com.groupon.sparklint.common

import java.util.TimerTask

import scala.util.{Failure, Success, Try}

/**
  * A convenient class to represent a job to be scheduled
  *
  * @param name          the name of the task
  * @param fn            task function
  * @param periodSeconds schedule interval
  * @param delaySeconds  initial job delay
  * @param logger        the logger used for logging
  * @tparam T the type of the task input
  * @author swhitear
  * @since 9/15/16.
  */
case class ScheduledTask[T](name: String, fn: () => Unit,
                            periodSeconds: Int = 1, delaySeconds: Int = 0)
                           (implicit logger: Logging)
  extends TimerTask {

  override def run(): Unit = Try({
    logger.logInfo(s"Executing ScheduledTask $name.")
    fn()
  }) match {
    case Success(_)  => logger.logInfo(s"Execution of $name completed")
    case Failure(ex) => logger.logError(s"Execution of $name failed with exception.", ex)
  }
}
