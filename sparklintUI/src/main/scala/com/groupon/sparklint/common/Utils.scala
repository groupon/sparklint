package com.groupon.sparklint.common

/**
  * @author rxue
  * @since 8/31/16.
  */
object Utils {
  val LOCALITIES         : Seq[Symbol] = Seq('PROCESS_LOCAL, 'NODE_LOCAL, 'NO_PREF, 'RACK_LOCAL, 'ANY)
  val UNKNOWN_STRING     : String      = "<unknown>"
  val UNKNOWN_NUMBER     : Long        = 0
  val STANDARD_APP_PREFIX: String      = "application_"
}
