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

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

import io.entilzha.spark.s3.S3Context.implicits._

class S3ContextSpec extends FlatSpec with Matchers with BeforeAndAfter {
  private val master = "local[*]"
  private val appName = "spark-s3 tests"
  private var sc: SparkContext = _

  before {
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    sc = new SparkContext(conf)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

  val lines = Seq("test line 0", "test line 1", "test line 2")

  it should "Read plain text from S3" in {
    val s3Lines = sc.s3.textFileByPrefix("entilzha.io", "spark-s3/test-text.txt").collect()
    s3Lines should equal(lines)
  }

  it should "Read compressed text from S3" in {
    val s3Lines = sc.s3.textFileByPrefix("entilzha.io", "spark-s3/test-gzip.txt.gz").collect()
    s3Lines should equal(lines)
  }
}
