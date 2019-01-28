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

package alluxio.job;

import alluxio.Constants;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.PropertyKey;
import alluxio.client.file.FileSystem;
import alluxio.conf.ServerConfiguration;
import alluxio.job.util.JobTestUtils;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.master.job.JobMaster;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.IntegrationTestUtils;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.network.NetworkAddressUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import java.net.ServerSocket;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Prepares the environment for the job manager integration tests.
 */
public abstract class JobIntegrationTest extends BaseIntegrationTest {
  protected static final int BUFFER_BYTES = 100;
  protected static final long WORKER_CAPACITY_BYTES = Constants.GB;
  protected static final int BLOCK_SIZE_BYTES = 128;

  protected JobMaster mJobMaster;
  protected FileSystem mFileSystem = null;
  protected FileSystemContext mFsContext;
  protected LocalAlluxioJobCluster mLocalAlluxioJobCluster;
  protected Map<NetworkAddressUtils.ServiceType, ServerSocket> mServiceMapping;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource = buildLocalAlluxioCluster();

  protected void customizeClusterResource(LocalAlluxioClusterResource.Builder resource) {
    mServiceMapping = IntegrationTestUtils.createMasterServiceMapping();
    resource.setSockets(mServiceMapping.get(NetworkAddressUtils.ServiceType.MASTER_RPC),
        mServiceMapping.get(NetworkAddressUtils.ServiceType.MASTER_WEB));
  }

  private LocalAlluxioClusterResource buildLocalAlluxioCluster() {
    LocalAlluxioClusterResource.Builder resource = new LocalAlluxioClusterResource.Builder()
        .setProperty(PropertyKey.JOB_MASTER_WORKER_HEARTBEAT_INTERVAL_MS, 20)
        .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE_BYTES)
        .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, String.valueOf(BUFFER_BYTES))
        .setProperty(PropertyKey.USER_NETWORK_READER_CHUNK_SIZE_BYTES, "64KB")
        .setProperty(PropertyKey.WORKER_MEMORY_SIZE, WORKER_CAPACITY_BYTES);
    customizeClusterResource(resource);
    return resource.build();
  }

  @Before
  public void before() throws Exception {
    mLocalAlluxioJobCluster = new LocalAlluxioJobCluster(
        mServiceMapping.get(NetworkAddressUtils.ServiceType.JOB_MASTER_RPC),
        mServiceMapping.get(NetworkAddressUtils.ServiceType.JOB_MASTER_WEB));
    mLocalAlluxioJobCluster.start();
    mJobMaster = mLocalAlluxioJobCluster.getMaster().getJobMaster();
    mFsContext = FileSystemContext.create(ServerConfiguration.global());
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
  }

  @After
  public void after() throws Exception {
    mFsContext.close();
    mLocalAlluxioJobCluster.stop();
  }

  protected JobInfo waitForJobToFinish(final long jobId)
      throws InterruptedException, TimeoutException {
    return JobTestUtils.waitForJobStatus(mJobMaster, jobId, Status.COMPLETED);
  }

  protected JobInfo waitForJobFailure(final long jobId)
      throws InterruptedException, TimeoutException {
    return JobTestUtils.waitForJobStatus(mJobMaster, jobId, Status.FAILED);
  }

  protected JobInfo waitForJobCancelled(final long jobId)
      throws InterruptedException, TimeoutException {
    return JobTestUtils.waitForJobStatus(mJobMaster, jobId, Status.CANCELED);
  }

  protected JobInfo waitForJobRunning(final long jobId)
      throws InterruptedException, TimeoutException {
    return JobTestUtils.waitForJobStatus(mJobMaster, jobId, Status.RUNNING);
  }
}
