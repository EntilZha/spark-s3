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

import org.apache.spark.{TaskContext, Partition, SparkContext}
import org.apache.spark.rdd.RDD


class S3Partition(keyIndex: Int, key: String) extends Partition {
  override def index: Int = keyIndex
}

class S3RDD(@transient sc: SparkContext) extends RDD[String](sc, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[String] = ???

  override protected def getPartitions: Array[Partition] = ???
}
