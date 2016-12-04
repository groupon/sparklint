/*
 * Copyright 2016 Groupon, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.groupon.sparklint.events

import com.groupon.sparklint.data.SparklintStateLike

/**
  * Extenders of the EventStateLike interface are capable of building current state based on the receiving
  * or removal of specific SparkListenerEvent messages.
  *
  * @author swhitear
  * @since 9/10/16.
  */
trait EventStateManagerLike {

  /**
    * The current EventState represented as a SparklintState object.
    *
    * @return the SparklintState object representing current state.
    */
  def getState: SparklintStateLike
}
