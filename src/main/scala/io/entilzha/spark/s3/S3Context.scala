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

import org.apache.spark.SparkContext


object S3Context {
  def apply(sc: SparkContext) = new S3Context(sc)
  object implicits {
    implicit def sparkContextToS3ContextWrapper(sc: SparkContext): S3ContextWrapper = {
      new S3ContextWrapper(sc)
    }
  }

  class S3ContextWrapper(@transient sc: SparkContext) {
    val s3 = S3Context(sc)
  }
}


class S3Context(@transient sc: SparkContext) extends Serializable {
  @scala.annotation.varargs
  def textFileByPrefix(bucket: String, prefix: String, additionalPrefixes: String*): S3RDD = {
    new S3RDD(sc, bucket, prefix +: additionalPrefixes)
  }
}
