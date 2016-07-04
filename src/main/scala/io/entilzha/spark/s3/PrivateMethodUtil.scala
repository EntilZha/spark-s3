/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.entilzha.spark.s3


private [s3] object PrivateMethodUtil {

  class PrivateMethodCaller(x: AnyRef, methodName: String) {
    def apply(_args: Any*): Any = {
      val args = _args.map(_.asInstanceOf[AnyRef])
      def _parents: Stream[Class[_]] = Stream(x.getClass) #::: _parents.map(_.getSuperclass)
      val parents = _parents.takeWhile(_ != null).toList
      val methods = parents.flatMap(_.getDeclaredMethods)
      val method = methods.find(_.getName == methodName).getOrElse(
        throw new IllegalArgumentException("Method " + methodName + " not found"))
      method.setAccessible(true)
      method.invoke(x, args: _*)
    }
  }

  class PrivateMethodExposer(x: AnyRef) {
    def apply(method: scala.Symbol): PrivateMethodCaller = new PrivateMethodCaller(x, method.name)
  }

  def p(x: AnyRef): PrivateMethodExposer = new PrivateMethodExposer(x)
}
