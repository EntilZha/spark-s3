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

import org.scalatest.{Matchers, FlatSpec}

import io.entilzha.spark.s3.LPTAlgorithm.calculateOptimalPartitions

class LPTAlgorithmSpec extends FlatSpec with Matchers {
  it should "balance partitions by file size" in {
    // This example was worked out on paper
    val sizes: Seq[Long] = Seq(
      10, 20, 5, 0, 1, 1, 0, 15, 18, 14, 35, 5, 8, 10, 14, 13, 17, 19, 0, 2
    )
    val files = sizes.map(s => (s.toString, s))
    val nPartitions = 5
    val partitions = calculateOptimalPartitions(files, nPartitions)
      .map(p => (p._1, p._2.length)).sorted
    val expectedPartitions = Seq((41L, 5), (41L, 4), (41L, 5), (42L, 3), (42L, 3)).sorted
    partitions should equal(expectedPartitions)
  }
  it should "balance partitions by number of files when partition sizes are similar" in {
    val files = Seq(
      ("a", 0L),
      ("b", 1L),
      ("c", 1L),
      ("d", 0L),
      ("e", 0L),
      ("f", 1L)
    )
    calculateOptimalPartitions(files, 3, 3).foreach { p =>
      assert(p._2.length == 2)
    }
  }
}
