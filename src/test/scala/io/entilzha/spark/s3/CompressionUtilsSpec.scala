package io.entilzha.spark.s3

import org.scalatest.{Matchers, FlatSpec}

class CompressionUtilsSpec extends FlatSpec with Matchers {
  val lines = Seq("test line 0", "test line 1", "test line 2")
  it should "be able to read text files" in {
    val stream = getClass.getResourceAsStream("/test.txt")
    CompressionUtils.decompress(stream).toSeq should equal(lines)
  }

  it should "be able to read gzip files" in {
    val stream = getClass.getResourceAsStream("/test.txt.gz")
    CompressionUtils.decompress(stream).toSeq should equal(lines)
  }
}
