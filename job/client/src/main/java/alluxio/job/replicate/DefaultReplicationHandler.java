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

package alluxio.job.replicate;

import alluxio.AlluxioURI;
import alluxio.client.job.JobMasterClient;
import alluxio.client.job.JobMasterClientPool;
import alluxio.exception.AlluxioException;

import java.io.IOException;

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
  public long evict(AlluxioURI uri, long blockId, int numReplicas)
      throws AlluxioException, IOException {
    JobMasterClient client = mJobMasterClientPool.acquire();
    try {
      return client.run(new EvictConfig(blockId, numReplicas));
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
}
