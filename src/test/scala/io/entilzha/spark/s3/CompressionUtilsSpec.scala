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

import scala.io.Source

class CompressionUtilsSpec extends FlatSpec with Matchers {
  val lines = Seq("test line 0", "test line 1", "test line 2")
  it should "be able to read text files" in {
    val stream = getClass.getResourceAsStream("/test.txt")
    Source.fromInputStream(CompressionUtils.decompress(stream)).getLines.toSeq should equal(lines)
  }

  it should "be able to read gzip files" in {
    val stream = getClass.getResourceAsStream("/test.txt.gz")
    Source.fromInputStream(CompressionUtils.decompress(stream)).getLines.toSeq should equal(lines)
  }
}
