/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.lineage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.IntegrationTestConstants;
import tachyon.IntegrationTestUtils;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.WriteType;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.FileSystem;
import tachyon.client.file.FileSystemMasterClient;
import tachyon.client.file.URIStatus;
import tachyon.client.file.options.CreateFileOptions;
import tachyon.client.lineage.LineageFileSystem;
import tachyon.client.lineage.LineageMasterClient;
import tachyon.client.lineage.TachyonLineage;
import tachyon.client.lineage.options.DeleteLineageOptions;
import tachyon.conf.TachyonConf;
import tachyon.heartbeat.HeartbeatContext;
import tachyon.heartbeat.HeartbeatScheduler;
import tachyon.job.CommandLineJob;
import tachyon.job.JobConf;
import tachyon.master.file.meta.PersistenceState;
import tachyon.util.CommonUtils;
import tachyon.wire.LineageInfo;

/**
 * Integration tests for the lineage module.
 */
public final class LineageMasterIntegrationTest {
  private static final int BLOCK_SIZE_BYTES = 128;
  private static final long WORKER_CAPACITY_BYTES = Constants.GB;
  private static final int QUOTA_UNIT_BYTES = 128;
  private static final int BUFFER_BYTES = 100;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource = new LocalTachyonClusterResource(
      WORKER_CAPACITY_BYTES, BLOCK_SIZE_BYTES,
      Constants.USER_FILE_BUFFER_BYTES, String.valueOf(BUFFER_BYTES),
      Constants.WORKER_DATA_SERVER, IntegrationTestConstants.NETTY_DATA_SERVER,
      Constants.USER_LINEAGE_ENABLED, "true",
      Constants.MASTER_LINEAGE_RECOMPUTE_INTERVAL_MS, "1000");

  private static final String OUT_FILE = "/test";
  private TachyonConf mTestConf;
  private CommandLineJob mJob;

  @BeforeClass
  public static void beforeClass() {
    HeartbeatContext.setTimerClass(HeartbeatContext.MASTER_CHECKPOINT_SCHEDULING,
        HeartbeatContext.SCHEDULED_TIMER_CLASS);
    HeartbeatContext.setTimerClass(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC,
        HeartbeatContext.SCHEDULED_TIMER_CLASS);
  }

  @Before
  public void before() throws Exception {
    mJob = new CommandLineJob("test", new JobConf("output"));
    mTestConf = mLocalTachyonClusterResource.get().getMasterTachyonConf();
  }

  @Test
  public void lineageCreationTest() throws Exception {
    LineageMasterClient lineageMasterClient = getLineageMasterClient();

    try {
      lineageMasterClient.createLineage(Lists.<String>newArrayList(), Lists.newArrayList(OUT_FILE),
          mJob);

      List<LineageInfo> infos = lineageMasterClient.getLineageInfoList();
      Assert.assertEquals(1, infos.size());
      TachyonURI uri = new TachyonURI(infos.get(0).getOutputFiles().get(0));
      URIStatus status = getFileSystemMasterClient().getStatus(uri);
      Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), status.getPersistenceState());
      Assert.assertFalse(status.isCompleted());
    } finally {
      lineageMasterClient.close();
    }
  }

  @Test
  public void lineageCompleteAndAsyncPersistTest() throws Exception {
    LineageMasterClient lineageMasterClient = getLineageMasterClient();

    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.MASTER_CHECKPOINT_SCHEDULING, 5,
        TimeUnit.SECONDS));
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC, 5,
        TimeUnit.SECONDS));

    try {
      lineageMasterClient.createLineage(Lists.<String>newArrayList(), Lists.newArrayList(OUT_FILE),
          mJob);

      CreateFileOptions options =
          CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE)
              .setBlockSizeBytes(BLOCK_SIZE_BYTES);
      LineageFileSystem fs = (LineageFileSystem) mLocalTachyonClusterResource.get().getClient();
      FileOutStream outputStream = fs.createFile(new TachyonURI(OUT_FILE), options);
      outputStream.write(1);
      outputStream.close();

      List<LineageInfo> infos = lineageMasterClient.getLineageInfoList();
      TachyonURI uri = new TachyonURI(infos.get(0).getOutputFiles().get(0));
      URIStatus status = getFileSystemMasterClient().getStatus(uri);
      Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), status.getPersistenceState());
      Assert.assertTrue(status.isCompleted());

      // Execute the checkpoint scheduler for async checkpoint
      HeartbeatScheduler.schedule(HeartbeatContext.MASTER_CHECKPOINT_SCHEDULING);
      Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.MASTER_CHECKPOINT_SCHEDULING, 5,
          TimeUnit.SECONDS));
      HeartbeatScheduler.schedule(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC);
      Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC, 5,
          TimeUnit.SECONDS));

      status = getFileSystemMasterClient().getStatus(uri);
      Assert.assertEquals(PersistenceState.IN_PROGRESS.toString(), status.getPersistenceState());

      IntegrationTestUtils.waitForPersist(mLocalTachyonClusterResource, status.getFileId());

      // worker notifies the master
      HeartbeatScheduler.schedule(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC);
      Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC, 5,
          TimeUnit.SECONDS));

      status = getFileSystemMasterClient().getStatus(uri);
      Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());

    } finally {
      lineageMasterClient.close();
    }
  }

  /**
   * Tests that a lineage job is executed when the output file for the lineage is reported as lost.
   */
  @Test(timeout = 20000)
  public void lineageRecoveryTest() throws Exception {
    File logFile = mFolder.newFile();
    // Delete the log file so that when it starts to exist we know that it was created by the
    // lineage recompute job
    logFile.delete();
    LineageMasterClient lineageClient = getLineageMasterClient();
    FileSystem fs = FileSystem.Factory.get();
    try {
      lineageClient.createLineage(ImmutableList.<String>of(), ImmutableList.of("/testFile"),
          new CommandLineJob("echo hello world", new JobConf(logFile.getAbsolutePath())));
      FileOutStream out = fs.createFile(new TachyonURI("/testFile"));
      out.write("foo".getBytes());
      out.close();
      lineageClient.reportLostFile("/testFile");
      // Wait for the log file to be created by the recompute job
      while (!logFile.exists()) {
        CommonUtils.sleepMs(20);
      }
      // Wait for the output to be written (this should be very fast)
      CommonUtils.sleepMs(10);
      BufferedReader reader = new BufferedReader(new FileReader(logFile));
      try {
        Assert.assertEquals("hello world", reader.readLine());
      } finally {
        reader.close();
      }
    } finally {
      lineageClient.close();
    }
  }

  /**
   * Runs code given in the docs (http://tachyon-project.org/documentation/Lineage-API.html) to make
   * sure it actually runs.
   *
   * If you need to update the doc-code here, make sure you also update it in the docs.
   */
  @Test
  public void docExampleTest() throws Exception {
    // create input files
    FileSystem fs = FileSystem.Factory.get();
    fs.createFile(new TachyonURI("/inputFile1")).close();
    fs.createFile(new TachyonURI("/inputFile2")).close();

    // ------ code block from docs ------
    TachyonLineage tl = TachyonLineage.get();
    // input file paths
    TachyonURI input1 = new TachyonURI("/inputFile1");
    TachyonURI input2 = new TachyonURI("/inputFile2");
    List<TachyonURI> inputFiles = Lists.newArrayList(input1, input2);
    // output file paths
    TachyonURI output = new TachyonURI("/outputFile");
    List<TachyonURI> outputFiles = Lists.newArrayList(output);
    // command-line job
    JobConf conf = new JobConf("/tmp/recompute.log");
    CommandLineJob job = new CommandLineJob("my-spark-job.sh", conf);
    long lineageId = tl.createLineage(inputFiles, outputFiles, job);

    // ------ code block from docs ------
    DeleteLineageOptions options = DeleteLineageOptions.defaults().setCascade(true);
    tl.deleteLineage(lineageId);

    fs.delete(new TachyonURI("/outputFile"));
    lineageId = tl.createLineage(inputFiles, outputFiles, job);

    // ------ code block from docs ------
    tl.deleteLineage(lineageId, options);
  }

  private LineageMasterClient getLineageMasterClient() {
    return new LineageMasterClient(mLocalTachyonClusterResource.get().getMaster().getAddress(),
        mTestConf);
  }

  private FileSystemMasterClient getFileSystemMasterClient() {
    return new FileSystemMasterClient(mLocalTachyonClusterResource.get().getMaster().getAddress(),
        mTestConf);
  }
}
