package com.groupon.sparklint.data

/**
  * An interval
  *
  * @author rxue
  * @since 12/4/16.
  */
case class Interval(minimum: Long, maximum: Long) {
  def merge(that: Interval): Interval = {
    Interval(minimum min that.minimum, maximum max that.maximum)
  }

  def length: Long = maximum - minimum
}
