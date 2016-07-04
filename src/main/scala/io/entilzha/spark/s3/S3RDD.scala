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

import org.apache.spark.executor.{InputMetrics, DataReadMethod}

import scala.collection.JavaConverters._

import com.amazonaws.auth.{DefaultAWSCredentialsProviderChain, BasicAWSCredentials}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ListObjectsRequest, S3ObjectSummary}

import org.apache.spark.{InterruptibleIterator, TaskContext, Partition, SparkContext}
import org.apache.spark.rdd.RDD


private [s3] class S3Partition(partitionIndex: Int,
                               val keys: Seq[String],
                               val size: Long) extends Partition {
  override def index: Int = partitionIndex
}

class S3RDD(@transient sc: SparkContext,
            bucket: String,
            prefixes: Seq[String],
            defaultNumPartitions: Int) extends RDD[String](sc, Nil) {
  private val accessKeyId = sc.hadoopConfiguration.get("fs.s3.awsAccessKeyId") match {
    case null => None
    case "" => None
    case s => Some(s)
  }
  private val secretAccessKey = sc.hadoopConfiguration.get("fs.s3.awsSecretAccessKey") match {
    case null => None
    case "" => None
    case s => Some(s)
  }

  /**
    * Attempt to create an Amazon S3 client. We first check to see if the Spark Hadoop configuration
    * has AWS keys otherwise default to the DefaultAWSCredentialsProviderChain which looks for
    * credentials in various default locations.
    *
    * @return AmazonS3Client configured with AWS keys
    */
  private def createS3Client(): AmazonS3Client = {
    val credentials = (accessKeyId, secretAccessKey) match {
      case (Some(key), Some(secret)) => new BasicAWSCredentials(key, secret)
      case _ => new DefaultAWSCredentialsProviderChain().getCredentials
    }
    new AmazonS3Client(credentials)
  }

  /**
    * Given the bucket and a list of prefixes find all matching objects and return their summaries
    *
    * @param bucket S3 Bucket to search
    * @param prefixes Varargs of string prefixes to filter by
    * @return S3 object summaries matching the bucket and prefixes
    */
  private def listSummaries(bucket: String, prefixes: Seq[String]): Array[S3ObjectSummary] = {
    val prefixSeq = prefixes.length match {
      case 0 | 1 => prefixes
      case _ => prefixes.par
    }
    prefixSeq.flatMap { prefix =>
      val request = new ListObjectsRequest()
      request.setBucketName(bucket)
      request.setPrefix(prefix)
      request.setMaxKeys(Int.MaxValue)
      val client = createS3Client()
      var objects = client.listObjects(request)
      var summaries = objects.getObjectSummaries.asScala.toSeq
      while (objects.isTruncated) {
        objects = client.listNextBatchOfObjects(objects)
        summaries ++= objects.getObjectSummaries.asScala.toSeq
      }
      summaries
    }.toArray
  }

  override def compute(partition: Partition, context: TaskContext): Iterator[String] = {
    val s3Partition = partition.asInstanceOf[S3Partition]
    val iter = new Iterator[String] {
      val taskMetrics = context.taskMetrics()

      // Spark Hadoop RDDs use getInputMetricsForReadMethod to help set input metrics
      // Unfortunately that method is private and the only way to set inputMetrics to a non-None
      // value. Therefore, it is necessary to use reflection to enable a call to the private method
      val inputMetrics = PrivateMethodUtil.p(taskMetrics)(
        'getInputMetricsForReadMethod)(DataReadMethod.Network).asInstanceOf[InputMetrics]
      inputMetrics.setBytesReadCallback(Some(() => {
        s3Partition.size
      }))
      context.addTaskCompletionListener(context => close())

      val client = createS3Client()
      val s3Iter = s3Partition.keys.iterator.flatMap { key =>
        CompressionUtils.decompress(client.getObject(bucket, key).getObjectContent)
      }
      val reader = new InterruptibleIterator[String](context, s3Iter)

      override def hasNext: Boolean = reader.hasNext

      override def next(): String = reader.next()

      private def close() = {
        inputMetrics.updateBytesRead()
      }
    }
    iter
  }

  override protected def getPartitions: Array[Partition] = {
    val summaries = listSummaries(bucket, prefixes)
    if (summaries.length <= defaultNumPartitions) {
      summaries.map(s => (s.getKey, s.getSize)).zipWithIndex.map {
        case ((key, size), i) => new S3Partition(i, Seq(key), size)
      }.toArray
    } else {
      val files = summaries.map(f => (f.getKey, f.getSize))
      val partitions = LPTAlgorithm.calculateOptimalPartitions(files, defaultNumPartitions)
      partitions.zipWithIndex.map { case ((size, keys), i) => new S3Partition(i, keys, size)}.toArray
    }
  }
}
