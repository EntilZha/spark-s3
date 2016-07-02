package io.entilzha.spark.s3

import scala.io.Source

import java.io.{BufferedInputStream, InputStream}

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
      case e: Exception => throw e
    }
  }
}
