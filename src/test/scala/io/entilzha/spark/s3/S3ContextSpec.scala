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
  private val master = "local[4]"
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

  val lines = Array("test line 0", "test line 1", "test line 2")

  it should "Read plain text from S3" in {
    val s3Lines = sc.s3.textFileByPrefix("entilzha.io", "spark-s3/test-text.txt").collect()
    s3Lines should equal(lines)
  }

  it should "Read compressed text from S3" in {
    val s3Lines = sc.s3.textFileByPrefix("entilzha.io", "spark-s3/test-gzip.txt.gz").collect()
    s3Lines should equal(lines)
  }

  it should "balance files across partitions" in {
    val partitionSizes = sc.s3.textFileByPrefix(
      "entilzha.io",
      "spark-s3/test-text.txt",
      "spark-s3/test-text.txt",
      "spark-s3/test-text.txt",
      "spark-s3/test-text.txt",
      "spark-s3/test-text.txt",
      "spark-s3/test-text.txt",
      "spark-s3/test-text.txt",
      "spark-s3/test-text.txt"
    ).glom.collect().map(_.length)
    // Since these files have exactly equal sizes, they should be put in 2 buckets so distinct
    // their length should be 1.
    assert(partitionSizes.distinct.length == 1)
  }

  // This runs against S3 files that are of the following sizes:
  // 2 1000KB files, 2 500KB files, 10 100KB files

  // Files were generated with:
  // $ base64 /dev/urandom | head -c 500000 > file.txt
  it should "balance files by size across partitions" in {
    val partitionSizes = sc.s3.textFileByPrefix(
      "entilzha.io",
      "spark-s3/size-tests"
    ).glom.map(_.length).collect()

    // Line count checks were done by hand and inserted here
    partitionSizes.foreach { size =>
      assert(size == 12989 || size == 12988)
    }
  }
}
