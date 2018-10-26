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

package alluxio.client.job;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.resource.ResourcePool;
import alluxio.worker.job.JobMasterClientConfig;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for managing job master clients. After obtaining a client with
 * {@link ResourcePool#acquire()}, {@link ResourcePool#release(Object)} must be called when the
 * thread is done using the client.
 */
@ThreadSafe
public final class JobMasterClientPool extends ResourcePool<JobMasterClient> {
  private final Queue<JobMasterClient> mClientList;

  /**
   * Creates a new job master client pool.
   */
  public JobMasterClientPool() {
    super(Configuration.getInt(PropertyKey.JOB_MASTER_CLIENT_THREADS));
    mClientList = new ConcurrentLinkedQueue<>();
  }

  @Override
  public void close() throws IOException {
    JobMasterClient client;
    while ((client = mClientList.poll()) != null) {
      client.close();
    }
  }

  @Override
  protected JobMasterClient createNewResource() {
    JobMasterClient client = JobMasterClient.Factory.create(JobMasterClientConfig.defaults());
    mClientList.add(client);
    return client;
  }
}
