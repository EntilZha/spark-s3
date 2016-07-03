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


import java.io.{BufferedInputStream, InputStream}

import scala.io.Source

import org.apache.commons.compress.compressors.{CompressorException, CompressorStreamFactory}


private [s3] object CompressionUtils {
  def decompress(input: InputStream): Iterator[String] = {
    val bufferedInput = new BufferedInputStream(input)
    try {
      val compressedInput = new CompressorStreamFactory().createCompressorInputStream(bufferedInput)
      Source.fromInputStream(compressedInput).getLines
    } catch {
      case compressException: CompressorException =>
        bufferedInput.reset()
        val compressedInput = bufferedInput
        Source.fromInputStream(compressedInput).getLines
    }
  }
}
