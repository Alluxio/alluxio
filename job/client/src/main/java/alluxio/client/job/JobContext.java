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

import alluxio.ClientContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.UnavailableException;
import alluxio.master.MasterInquireClient;
import alluxio.resource.CloseableResource;
import alluxio.security.user.UserState;
import alluxio.worker.job.JobMasterClientContext;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A shared context that isolates all operations within the same JVM. Usually, one user only needs
 * one instance of {@link JobContext} for interacting with job service.
 *
 * <p>
 * NOTE: The context maintains a pool of job master clients that is already thread-safe.
 * Synchronizing {@link JobContext} methods could lead to deadlock: thread A attempts to acquire a
 * client when there are no clients left in the pool and blocks holding a lock on the
 * {@link JobContext}, when thread B attempts to release a client it owns it is unable to do so,
 * because thread A holds the lock on {@link JobContext}.
 */
@ThreadSafe
public final class JobContext implements Closeable  {
  /** The shared master inquire client associated with the {@link JobContext}. */
  @GuardedBy("this")
  private MasterInquireClient mJobMasterInquireClient;
  private volatile JobMasterClientPool mJobMasterClientPool;

  /**
   * Creates a job context.
   *
   * @param alluxioConf Alluxio configuration
   * @param userState user state
   * @return the context
   */
  public static JobContext create(AlluxioConfiguration alluxioConf, UserState userState) {
    JobContext context = new JobContext();
    context.init(alluxioConf, userState);
    return context;
  }

  /**
   * Create a job context.
   */
  private JobContext() {} // private constructor to prevent instantiation

  /**
   * Initializes the context. Only called in the factory methods and reset.
   */
  private synchronized void init(AlluxioConfiguration alluxioConf, UserState userState) {
    mJobMasterInquireClient = MasterInquireClient.Factory
        .createForJobMaster(alluxioConf, userState);
    mJobMasterClientPool =
        new JobMasterClientPool(JobMasterClientContext
            .newBuilder(ClientContext.create(userState.getSubject(), alluxioConf)).build());
  }

  /**
   * Closes all the resources associated with the context. Make sure all the resources are released
   * back to this context before calling this close. After closing the context, all the resources
   * that acquired from this context might fail.
   */
  @Override
  public void close() throws IOException {
    mJobMasterClientPool.close();
    mJobMasterClientPool = null;

    synchronized (this) {
      mJobMasterInquireClient = null;
    }
  }

  /**
   * @return the job master address
   * @throws UnavailableException if the master address cannot be determined
   */
  public synchronized InetSocketAddress getJobMasterAddress() throws UnavailableException {
    return mJobMasterInquireClient.getPrimaryRpcAddress();
  }

  /**
   * Acquires a job master client from the job master client pool.
   *
   * @return the acquired job master client
   */
  public JobMasterClient acquireMasterClient() {
    return mJobMasterClientPool.acquire();
  }

  /**
   * Releases a job master client into the job master client pool.
   *
   * @param masterClient a job master client to release
   */
  public void releaseMasterClient(JobMasterClient masterClient) {
    mJobMasterClientPool.release(masterClient);
  }

  /**
   * Acquires a job master client from the job master client pool. The resource is
   * {@code Closeable}.
   *
   * @return the acquired job master client resource
   */
  public CloseableResource<JobMasterClient> acquireMasterClientResource() {
    return new CloseableResource<JobMasterClient>(mJobMasterClientPool.acquire()) {
      @Override
      public void close() {
        mJobMasterClientPool.release(get());
      }
    };
  }
}
