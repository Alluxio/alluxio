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

import static org.mockito.Mockito.mock;

import alluxio.AlluxioURI;
import alluxio.TestLoggerRule;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.WritePType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.job.JobIntegrationTest;
import alluxio.master.job.plan.PlanTracker;
import alluxio.master.job.workflow.WorkflowTracker;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.io.BufferUtils;
import alluxio.wire.BlockInfo;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

/**
 * Integration tests for {@link ReplicateDefinition}.
 */
public final class ReplicateIntegrationTest extends JobIntegrationTest {

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule =
      new ManuallyScheduleHeartbeat(HeartbeatContext.MASTER_REPLICATION_CHECK);

  private static final String TEST_URI = "/test";
  private static final int TEST_BLOCK_SIZE = 100;
  private long mBlockId1;
  private long mBlockId2;

  @Rule
  public TestLoggerRule mLogger = new TestLoggerRule();

  @Before
  public void before() throws Exception {
    super.before();

    AlluxioURI filePath = new AlluxioURI(TEST_URI);
    // write a file outside of Alluxio
    createFileOutsideOfAlluxio(filePath);
    URIStatus status = mFileSystem.getStatus(filePath);
    mBlockId1 = status.getBlockIds().get(0);
    mBlockId2 = status.getBlockIds().get(1);
  }

  private void createFileOutsideOfAlluxio(AlluxioURI uri) throws Exception {
    try (FileOutStream os = mFileSystem.createFile(uri,
        CreateFilePOptions.newBuilder().setWriteType(WritePType.THROUGH)
            .setBlockSizeBytes(TEST_BLOCK_SIZE).setRecursive(true).build())) {
      os.write(BufferUtils.getIncreasingByteArray(TEST_BLOCK_SIZE + 1));
    }
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {PropertyKey.Name.JOB_MASTER_JOB_CAPACITY, "1",
      PropertyKey.Name.JOB_MASTER_FINISHED_JOB_RETENTION_TIME, "0"})
  public void replicateFullBlockFromUFS() throws Exception {
    // run the replicate job for mBlockId1
    // hack - use a job tracker with capacity of 1
    PlanTracker planTracker = new PlanTracker(1, 0, -1, mock(WorkflowTracker.class));
    Whitebox.setInternalState(mJobMaster, "mPlanTracker", planTracker);
    waitForJobToFinish(mJobMaster.run(new ReplicateConfig(TEST_URI, mBlockId1, 1)));

    BlockInfo blockInfo1 = AdjustJobTestUtils.getBlock(mBlockId1, mFsContext);
    BlockInfo blockInfo2 = AdjustJobTestUtils.getBlock(mBlockId2, mFsContext);
    Assert.assertEquals(1, blockInfo1.getLocations().size());
    Assert.assertEquals(0, blockInfo2.getLocations().size());
    Assert.assertEquals(TEST_BLOCK_SIZE, blockInfo1.getLength());
    Assert.assertEquals(1, blockInfo2.getLength());
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {PropertyKey.Name.JOB_MASTER_JOB_CAPACITY, "1",
      PropertyKey.Name.JOB_MASTER_FINISHED_JOB_RETENTION_TIME, "0"})
  public void replicateLastBlockFromUFS() throws Exception {
    // run the replicate job for mBlockId2
    // hack - use a plan tracker with capacity of 1
    PlanTracker planTracker = new PlanTracker(1, 0, -1, mock(WorkflowTracker.class));
    Whitebox.setInternalState(mJobMaster, "mPlanTracker", planTracker);
    waitForJobToFinish(mJobMaster.run(new ReplicateConfig(TEST_URI, mBlockId2, 1)));

    BlockInfo blockInfo1 = AdjustJobTestUtils.getBlock(mBlockId1, mFsContext);
    BlockInfo blockInfo2 = AdjustJobTestUtils.getBlock(mBlockId2, mFsContext);
    Assert.assertEquals(0, blockInfo1.getLocations().size());
    Assert.assertEquals(1, blockInfo2.getLocations().size());
    Assert.assertEquals(TEST_BLOCK_SIZE, blockInfo1.getLength());
    Assert.assertEquals(1, blockInfo2.getLength());
  }

  // Tests that we send one request and log one message when job master is at capacity
  @Test
  @LocalAlluxioClusterResource.Config(confParams = {PropertyKey.Name.JOB_MASTER_JOB_CAPACITY, "0",
      PropertyKey.Name.JOB_MASTER_FINISHED_JOB_RETENTION_TIME, "0"})
  public void requestBackoffTest() throws Exception {
    String rootDir = "/backofftest";
    for (int i = 0; i < 10; i++) {
      AlluxioURI uri = new AlluxioURI(rootDir + "/" + i);
      createFileOutsideOfAlluxio(uri);
    }
    SetAttributePOptions opts = SetAttributePOptions.newBuilder().setReplicationMin(2).build();
    mFileSystem.setAttribute(new AlluxioURI(rootDir), opts);
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_REPLICATION_CHECK);

    // After logging we expect only one log message to be logged as the job master has a zero job
    // capacity even though there should be 10 replication jobs.
    Assert.assertEquals(1, mLogger.logCount("The job service is busy, will retry later."));
    Assert.assertEquals(1, mLogger.logCount("Job master is at full capacity of 0 jobs"));
  }
}
