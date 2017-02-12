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

/**
  * A case class wrapping the three current event receivers, allows views to access by name.
  *
  * @param meta          an EventSourceMetaLike instance containing metadata about the application.
  * @param progress      an EventProgressTrackerLike instance containing progress of various event types.
  * @param state         an EventStateManagerLike containing the current aggregated event state.
  * @author swhitear
  * @since 9/13/16.
  *
  */
case class EventSourceDetail(meta: EventSourceMetaLike,
                             progress: EventProgressTrackerLike,
                             state: EventStateManagerLike)
