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
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.TestLoggerRule;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.exception.ExceptionMessage;
import alluxio.grpc.SetAttributePOptions;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.job.JobIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.io.BufferUtils;
import alluxio.wire.BlockInfo;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

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
        CreateFilePOptions.newBuilder().setWriteType(WritePType.WRITE_THROUGH)
            .setBlockSizeBytes(TEST_BLOCK_SIZE).setRecursive(true).build())) {
      os.write(BufferUtils.getIncreasingByteArray(TEST_BLOCK_SIZE + 1));
    }
  }

  @Test
  public void replicateFullBlockFromUFS() throws Exception {
    // run the replicate job for mBlockId1
    waitForJobToFinish(mJobMaster.run(new ReplicateConfig(TEST_URI, mBlockId1, 1)));

    BlockInfo blockInfo1 = AdjustJobTestUtils.getBlock(mBlockId1, FileSystemContext.get());
    BlockInfo blockInfo2 = AdjustJobTestUtils.getBlock(mBlockId2, FileSystemContext.get());
    Assert.assertEquals(1, blockInfo1.getLocations().size());
    Assert.assertEquals(0, blockInfo2.getLocations().size());
    Assert.assertEquals(TEST_BLOCK_SIZE, blockInfo1.getLength());
    Assert.assertEquals(1, blockInfo2.getLength());
  }

  @Test
  public void replicateLastBlockFromUFS() throws Exception {
    // run the replicate job for mBlockId2
    waitForJobToFinish(mJobMaster.run(new ReplicateConfig(TEST_URI, mBlockId2, 1)));

    BlockInfo blockInfo1 = AdjustJobTestUtils.getBlock(mBlockId1, FileSystemContext.get());
    BlockInfo blockInfo2 = AdjustJobTestUtils.getBlock(mBlockId2, FileSystemContext.get());
    Assert.assertEquals(0, blockInfo1.getLocations().size());
    Assert.assertEquals(1, blockInfo2.getLocations().size());
    Assert.assertEquals(TEST_BLOCK_SIZE, blockInfo1.getLength());
    Assert.assertEquals(1, blockInfo2.getLength());
  }

  // Tests that we send one request and log one message when job master is at capacity
  @Test
  @LocalAlluxioClusterResource.Config(confParams = {PropertyKey.Name.JOB_MASTER_JOB_CAPACITY, "0",
      PropertyKey.Name.JOB_MASTER_FINISHED_JOB_RETENTION_MS, "0"})
  public void requestBackoffTest() throws Exception {
    String rootDir = "/backofftest";
    for (int i = 0; i < 10; i++) {
      AlluxioURI uri = new AlluxioURI(rootDir + "/" + i);
      createFileOutsideOfAlluxio(uri);
    }
    String exceptionMsg = ExceptionMessage.JOB_MASTER_FULL_CAPACITY
        .getMessage(Configuration.get(PropertyKey.JOB_MASTER_JOB_CAPACITY));
    String replicationCheckerMsg = "The job service is busy, will retry later."
        + " alluxio.exception.status.ResourceExhaustedException: " + exceptionMsg;
    String rpcUtilsMsg = "Error=alluxio.exception.status.ResourceExhaustedException: "
        + exceptionMsg;
    SetAttributePOptions opts = SetAttributePOptions.newBuilder().setReplicationMin(2).build();
    mFileSystem.setAttribute(new AlluxioURI(rootDir), opts);
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_REPLICATION_CHECK);

    // After logging we expect only one log message to be logged as the job master has a zero job
    // capacity even though there should be 10 replication jobs.
    Assert.assertEquals(1, mLogger.logCount(replicationCheckerMsg));
    //Assert.assertEquals(1, mLogger.logCount(rpcUtilsMsg));
  }
}
