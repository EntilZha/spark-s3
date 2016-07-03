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

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import com.amazonaws.services.s3.model.{S3ObjectSummary, ListObjectsRequest}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.{DefaultAWSCredentialsProviderChain, BasicAWSCredentials}


object S3Context {
  def apply(sc: SparkContext) = new S3Context(sc)
  object implicits {
    implicit def sparkContextToS3ContextWrapper(sc: SparkContext): S3ContextWrapper = {
      new S3ContextWrapper(sc)
    }
  }

  class S3ContextWrapper(@transient sc: SparkContext) {
    val s3 = S3Context(sc)
  }
}


class S3Context(@transient sc: SparkContext) extends Serializable {
  val accessKeyId = sc.hadoopConfiguration.get("fs.s3.awsAccessKeyId") match {
    case null => None
    case "" => None
    case s => Some(s)
  }
  val secretAccessKey = sc.hadoopConfiguration.get("fs.s3.awsSecretAccessKey") match {
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
  private def listSummaries(bucket: String, prefixes: Seq[String]): Seq[S3ObjectSummary] = {
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
    }.seq
  }

  def textFileByPrefix(bucket: String, prefix: String, additionalPrefixes: String*): RDD[String] = {
    val prefixes = prefix +: additionalPrefixes
    val summaries = listSummaries(bucket, prefixes)
    val keys = summaries.map(_.getKey)
    val nPartitions = math.max(sc.defaultParallelism, keys.length)
    sc.parallelize(keys, nPartitions).flatMap { key =>
      val client = createS3Client()
      CompressionUtils.decompress(client.getObject(bucket, key).getObjectContent)
    }
  }
}
