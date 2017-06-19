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

package jawn.support.json4s

import jawn.{FContext, Facade, SupportParser}
import org.json4s.JsonAST._

import scala.collection.mutable

object Parser extends Parser(true, true)

class Parser(useBigDecimalForDouble: Boolean, useBigIntForLong: Boolean) extends SupportParser[JValue] {

  implicit val facade: Facade[JValue] =
    new Facade[JValue] {
      def jnull() = JNull
      def jfalse() = JBool(false)
      def jtrue() = JBool(true)
      def jnum(s: String) = if (useBigDecimalForDouble) JDecimal(BigDecimal(s)) else JDouble(s.toDouble)
      def jint(s: String) = if (useBigIntForLong) JInt(BigInt(s)) else JInt(s.toInt)
      def jstring(s: String) = JString(s)

      def singleContext() =
        new FContext[JValue] {
          var value: JValue = null
          def add(s: String) { value = jstring(s) }
          def add(v: JValue) { value = v }
          def finish: JValue = value
          def isObj: Boolean = false
        }

      def arrayContext() =
        new FContext[JValue] {
          val vs = mutable.ListBuffer.empty[JValue]
          def add(s: String) { vs += jstring(s) }
          def add(v: JValue) { vs += v }
          def finish: JValue = JArray(vs.toList)
          def isObj: Boolean = false
        }

      def objectContext() =
        new FContext[JValue] {
          var key: String = null
          val vs = mutable.ListBuffer.empty[JField]
          def add(s: String): Unit =
            if (key == null) key = s
            else { vs += JField(key, jstring(s)); key = null }
          def add(v: JValue): Unit =
          { vs += JField(key, v); key = null }
          def finish: JValue = JObject(vs.toList)
          def isObj: Boolean = true
        }
    }
}
