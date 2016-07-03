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

import scala.collection.mutable

/**
  * This implements the Longest Processing Time (LPT) algorithm for solving the multi-processor
  * scheduling problem. The optimal solution is NP-Hard, but LPT achieves a good bound of 4/3 OPT.
  *
  * Formally: Given a set J of jobs where job Ji has length Li, and a number of processors m, what
  * is the minimum possible time required to schedule all jobs in J on m processors such that none
  * overlap.
  *
  * For the purposes of [[io.entilzha.spark.s3.S3RDD]], jobs are S3 file keys, length is the file
  * size, and m is the number of partitions. The desired output is how to split the file keys such
  * that the partitions are as balanced as possible within the 4/3 OPT bound.
  *
  * References
  *
  * Algorithm Summary: [[https://en.wikipedia.org/wiki/Multiprocessor_scheduling]]
  *
  * Problem Formulation: Garey, Michael R.; Johnson, David S. Computers and Intractability: A Guide
  * to the Theory of NP-Completeness. W. H. Freeman and Company. p. 238 ISBN 0716710447.
  *
  * Optimality Paper: Graham, R. L. (1969). "Bounds on Multiprocessing Timing Anomalies". SIAM
  * Journal on Applied Mathematics 17 (2): 416-429. doi:10.1137/0117039
  */
private [s3] object LPTAlgorithm {
  private case class Partition(size: Long, files: Seq[String])

  /**
    * Given a list of files, their sizes, number of partitions to make, and a size error, group them
    * into that many partitions as evenly as possible using the LPT algorithm.
    *
    * @param files List of files and their sizes to partition
    * @param nPartitions Number of partitions to make
    * @param sizeError If two partition sizes are within this much of each other, then partition
    *                  length is used to compare. This is important to balance the number of low
    *                  file size numbers so one partition isn't stuck with a large overhead for many
    *                  small file requests.
    * @return Balanced partitions via the LPT algorithm
    */
  def calculateOptimalPartitions(files: Seq[(String, Long)],
                                 nPartitions: Int,
                                 sizeError: Long = 0): Seq[(Long, Seq[String])] = {
    def PartitionOrder = new Ordering[Partition] {
      def compare(left: Partition, right: Partition) = {
        if (math.abs(left.size - right.size) <= sizeError) {
          -left.files.length.compare(right.files.length)
        } else {
          (-left.size).compare(-right.size)
        }
      }
    }
    val partitions = List.fill(nPartitions)(Partition(0L, Seq.empty[String]))
    val queue = new mutable.PriorityQueue[Partition]()(PartitionOrder) ++ partitions
    files.sortBy(-_._2).foreach { case (file, size) =>
      val partition = queue.dequeue()
      queue.enqueue(Partition(partition.size + size, partition.files :+ file))
    }
    queue.toSeq.map(p => Partition.unapply(p).get)
  }
}
