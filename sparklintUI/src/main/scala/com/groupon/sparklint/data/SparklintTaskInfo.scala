package com.groupon.sparklint.data

/**
  * @author rxue
  * @since 9/23/16.
  */
case class SparklintTaskInfo(taskId: Long, executorId: String, index: Int, attemptNumber: Int, launchTime: Long, locality: Symbol, speculative: Boolean)
