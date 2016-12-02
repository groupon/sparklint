package com.groupon.sparklint.events

/**
  * Simple data structure to track event source playback progress
  * @author swhitear, rxue
  * @since 9/12/16.
  */
class EventProgress(var count: Int, var started: Int, var complete: Int, var active: Set[String]) {
  require(count >= 0)
  require(started >= 0 && started <= count)
  require(complete >= 0 && complete <= count)
  require(complete <= started)

  def safeCount: Double = if (count == 0) 1 else count

  def percent: Long = ((complete / safeCount) * 100).round

  def description = s"Completed $complete / $count ($percent%) with $inFlightCount active$activeString."

  def hasNext: Boolean = complete < count

  def hasPrevious: Boolean = complete > 0

  def inFlightCount: Int = started - complete

  override def toString: String = s"$complete of $count with $inFlightCount active"

  private def activeString = if (active.isEmpty) "" else s" (${active.mkString(", ")})"
}

object EventProgress {
  def empty(): EventProgress = new EventProgress(0, 0, 0, Set.empty)
}
