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

import java.io.InputStream

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.S3Object
import org.apache.spark.TaskContext
import org.apache.spark.executor.{InputMetrics, DataReadMethod}

import scala.io.Source

class S3Iterator(bucket: String,
                 client: AmazonS3Client,
                 partition: S3Partition,
                 context: TaskContext) extends Iterator[String]{
  val taskMetrics = context.taskMetrics()
  val inputMetrics = PrivateMethodUtil.p(taskMetrics)(
    'getInputMetricsForReadMethod)(DataReadMethod.Network).asInstanceOf[InputMetrics]
  inputMetrics.setBytesReadCallback(Some(() => {
    bytesRead()
  }))
  context.addTaskCompletionListener(context => close())

  val keys = partition.keys

  var reader: Iterator[String] = newIter(0)
  var iterIndex = 1
  var s3Object: S3Object = _
  var inputStream: InputStream = _
  var _bytesRead = 0L

  override def hasNext: Boolean = reader.hasNext

  override def next(): String = {
    val element = reader.next()
    if (!reader.hasNext) {
      initNextReader()
    }
    element
  }

  private def newIter(iterIndex: Int): Iterator[String] = {
    s3Object = client.getObject(bucket, keys(iterIndex))
    inputStream = CompressionUtils.decompress(s3Object.getObjectContent)
    Source.fromInputStream(inputStream).getLines
  }

  private def initNextReader() = {
    while (iterIndex < keys.length && !reader.hasNext) {
      _bytesRead += s3Object.getObjectMetadata.getContentLength
      inputStream.close()
      reader = newIter(iterIndex)
      iterIndex += 1
    }
  }

  private def bytesRead() = _bytesRead

  private def close() = {
    inputStream.close()
  }
}
