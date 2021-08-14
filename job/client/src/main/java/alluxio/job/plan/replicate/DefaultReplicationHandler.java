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
import alluxio.client.job.JobMasterClient;
import alluxio.client.job.JobMasterClientPool;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.ListAllPOptions;
import alluxio.job.wire.Status;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The implementation of {@link ReplicationHandler} that utilizes job service.
 */
@ThreadSafe
public final class DefaultReplicationHandler implements ReplicationHandler {
  private final JobMasterClientPool mJobMasterClientPool;

  /**
   * Creates a new instance of {@link DefaultReplicationHandler}.
   *
   * @param jobMasterClientPool job master client pool
   */
  public DefaultReplicationHandler(JobMasterClientPool jobMasterClientPool) {
    mJobMasterClientPool = jobMasterClientPool;
  }

  @Override
  public Status getJobStatus(long jobId) throws IOException {
    final JobMasterClient client = mJobMasterClientPool.acquire();
    try {
      return client.getJobStatus(jobId).getStatus();
    } catch (NotFoundException e) {
      // if the job status doesn't exist, assume the job has failed
      return Status.FAILED;
    } finally {
      mJobMasterClientPool.release(client);
    }
  }

  @Override
  public List<Long> findJobs(String jobName, Set<Status> status) throws IOException {
    final JobMasterClient client = mJobMasterClientPool.acquire();
    try {
      return client.list(ListAllPOptions.newBuilder().setName(jobName)
          .addAllStatus(status.stream().map(Status::toProto).collect(Collectors.toSet()))
          .build());
    } finally {
      mJobMasterClientPool.release(client);
    }
  }

  @Override
  public long evict(AlluxioURI uri, long blockId, int numReplicas)
      throws AlluxioException, IOException {
    JobMasterClient client = mJobMasterClientPool.acquire();
    try {
      return client.run(new EvictConfig(uri.getPath(), blockId, numReplicas));
    } finally {
      mJobMasterClientPool.release(client);
    }
  }

  @Override
  public long replicate(AlluxioURI uri, long blockId, int numReplicas)
      throws AlluxioException, IOException {
    JobMasterClient client = mJobMasterClientPool.acquire();
    try {
      return client.run(new ReplicateConfig(uri.getPath(), blockId, numReplicas));
    } finally {
      mJobMasterClientPool.release(client);
    }
  }

  @Override
  public long migrate(AlluxioURI uri, long blockId, String workerHost, String mediumType)
      throws AlluxioException, IOException {
    JobMasterClient client = mJobMasterClientPool.acquire();
    try {
      return client.run(new MoveConfig(uri.getPath(), blockId, workerHost, mediumType));
    } finally {
      mJobMasterClientPool.release(client);
    }
  }
}
