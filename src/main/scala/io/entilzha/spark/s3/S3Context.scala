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
  def apply(sc: SparkContext, defaultNumPartitions: Int) = new S3Context(sc, defaultNumPartitions)
  object implicits {
    implicit def sparkContextToS3ContextWrapper(sc: SparkContext): S3ContextWrapper = {
      new S3ContextWrapper(sc)
    }
  }

  class S3ContextWrapper(@transient sc: SparkContext) {
    def s3 = S3Context(sc)
    def s3(defaultNumPartitions: Int) = S3Context(sc, defaultNumPartitions)
  }
}


class S3Context(@transient sc: SparkContext, defaultNumPartitions: Int) extends Serializable {
  def this(@transient sc: SparkContext) {
    this(sc, sc.defaultParallelism)
  }
  /**
    * Basic entrypoint to Amazon S3 access. Requires a S3 bucket and at least one prefix to match on
    *
    * @param bucket Bucket to read from
    * @param prefix First prefix to match
    * @param additionalPrefixes Additional prefixes to match
    * @return [[S3RDD]] with contents of files with bucket and prefixes
    */
  @scala.annotation.varargs
  def textFileByPrefix(bucket: String, prefix: String, additionalPrefixes: String*): S3RDD = {
    new S3RDD(sc, bucket, prefix +: additionalPrefixes, defaultNumPartitions)
  }
}
