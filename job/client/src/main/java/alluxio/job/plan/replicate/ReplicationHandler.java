/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.job.plan.replicate;

import alluxio.AlluxioURI;
import alluxio.exception.AlluxioException;
import alluxio.job.wire.Status;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Interface for adjusting the replication level of blocks.
 */
public interface ReplicationHandler {

  /**
   * @param jobId the job id returned by evict, replicate, or migrate
   * @return the job status (running, completed, failed, etc.)
   * @throws IOException if a non-Alluxio error is encountered
   */
  Status getJobStatus(long jobId) throws IOException;

  /**
   * @param jobName name of the job
   * @param statusList job status
   * @return a list of job ids that match the criteria
   * @throws IOException if a non-Alluxio error is encountered
   */
  List<Long> findJobs(String jobName, Set<Status> statusList) throws IOException;

  /**
   * Decreases the block replication level by a target number of replicas.
   *
   * @param uri URI of the file the block belongs to
   * @param blockId ID of the block
   * @param numReplicas how many replicas to remove
   * @return the ID of the replicate job
   * @throws AlluxioException if an Alluxio error is encountered
   * @throws IOException if a non-Alluxio error is encountered
   */
  long evict(AlluxioURI uri, long blockId, int numReplicas) throws AlluxioException, IOException;

  /**
   * Increases the block replication level by a target number of replicas.
   *
   * @param uri URI of the file the block belongs to
   * @param blockId ID of the block
   * @param numReplicas how many replicas to add
   * @return the ID of the replicate job
   * @throws AlluxioException if an Alluxio error is encountered
   * @throws IOException if a non-Alluxio error is encountered
   */
  long replicate(AlluxioURI uri, long blockId, int numReplicas)
      throws AlluxioException, IOException;

  /**
   * Migrate blocks to the correctly pinned locations.
   *
   * @param uri URI of the file the block belongs to
   * @param blockId ID of the block
   * @param workerHost worker host this block is located at
   * @param mediumType medium type to migrate this block to
   * @return the ID of the replicate job
   * @throws AlluxioException if an Alluxio error is encountered
   * @throws IOException if a non-Alluxio error is encountered
   */
  long migrate(AlluxioURI uri, long blockId, String workerHost, String mediumType)
      throws AlluxioException, IOException;
}
