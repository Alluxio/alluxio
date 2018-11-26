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
import alluxio.PropertyKey;
import alluxio.client.file.FileSystem;
import alluxio.job.util.JobTestUtils;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.master.job.JobMaster;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

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
  protected LocalAlluxioJobCluster mLocalAlluxioJobCluster;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.JOB_MASTER_WORKER_HEARTBEAT_INTERVAL_MS, 20)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE_BYTES)
          .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, String.valueOf(BUFFER_BYTES))
          .setProperty(PropertyKey.USER_NETWORK_NETTY_READER_PACKET_SIZE_BYTES, "64KB")
          .setProperty(PropertyKey.WORKER_MEMORY_SIZE, WORKER_CAPACITY_BYTES)
          .build();

  @Before
  public void before() throws Exception {
    mLocalAlluxioJobCluster = new LocalAlluxioJobCluster();
    mLocalAlluxioJobCluster.start();
    mJobMaster = mLocalAlluxioJobCluster.getMaster().getJobMaster();
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
  }

  @After
  public void after() throws Exception {
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
