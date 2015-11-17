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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.IntegrationTestConstants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.WriteType;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.client.lineage.LineageMasterClient;
import tachyon.client.lineage.TachyonLineageFileSystem;
import tachyon.conf.TachyonConf;
import tachyon.heartbeat.HeartbeatContext;
import tachyon.heartbeat.HeartbeatScheduler;
import tachyon.job.CommandLineJob;
import tachyon.job.JobConf;
import tachyon.master.lineage.meta.LineageFileState;
import tachyon.thrift.LineageInfo;

/**
 * Integration tests for the lineage module.
 */
public final class LineageMastertIntegrationsTest {
  private static final int BLOCK_SIZE_BYTES = 128;
  private static final long WORKER_CAPACITY_BYTES = Constants.GB;
  private static final int QUOTA_UNIT_BYTES = 128;
  private static final int BUFFER_BYTES = 100;

  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource =
      new LocalTachyonClusterResource(WORKER_CAPACITY_BYTES, QUOTA_UNIT_BYTES, BLOCK_SIZE_BYTES,
          Constants.USER_FILE_BUFFER_BYTES, String.valueOf(BUFFER_BYTES),
          Constants.WORKER_DATA_SERVER, IntegrationTestConstants.NETTY_DATA_SERVER,
          Constants.USER_LINEAGE_ENABLED, "true");

  private static final String OUT_FILE = "/test";
  private TachyonConf mTestConf;
  private CommandLineJob mJob;

  @BeforeClass
  public static void beforeClass() {
    HeartbeatContext.setTimerClass(HeartbeatContext.MASTER_CHECKPOINT_SCHEDULING,
        HeartbeatContext.SCHEDULED_TIMER_CLASS);
    HeartbeatContext.setTimerClass(HeartbeatContext.WORKER_LINEAGE_SYNC,
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
      Assert.assertEquals(LineageFileState.CREATED.toString(),
          infos.get(0).outputFiles.get(0).state);
    } finally {
      lineageMasterClient.close();
    }
  }

  @Test
  public void lineageCompleteAndAsyncPersistTest() throws Exception {
    LineageMasterClient lineageMasterClient = getLineageMasterClient();

    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.MASTER_CHECKPOINT_SCHEDULING, 5,
        TimeUnit.SECONDS));
    Assert.assertTrue(
        HeartbeatScheduler.await(HeartbeatContext.WORKER_LINEAGE_SYNC, 5, TimeUnit.SECONDS));

    try {
      lineageMasterClient.createLineage(Lists.<String>newArrayList(), Lists.newArrayList(OUT_FILE),
          mJob);

      OutStreamOptions options = new OutStreamOptions.Builder(mTestConf)
          .setWriteType(WriteType.MUST_CACHE).setBlockSizeBytes(BLOCK_SIZE_BYTES).build();
      TachyonLineageFileSystem tfs =
          (TachyonLineageFileSystem) mLocalTachyonClusterResource.get().getClient();
      FileOutStream outputStream = tfs.getOutStream(new TachyonURI(OUT_FILE), options);
      outputStream.write(1);
      outputStream.close();

      List<LineageInfo> infos = lineageMasterClient.getLineageInfoList();
      Assert.assertEquals(LineageFileState.COMPLETED.toString(),
          infos.get(0).outputFiles.get(0).state);

      // Execute the checkpoint scheduler for async checkpoint
      HeartbeatScheduler.schedule(HeartbeatContext.MASTER_CHECKPOINT_SCHEDULING);
      HeartbeatScheduler.schedule(HeartbeatContext.WORKER_LINEAGE_SYNC);
      Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.MASTER_CHECKPOINT_SCHEDULING, 5,
          TimeUnit.SECONDS));
      Assert.assertTrue(
          HeartbeatScheduler.await(HeartbeatContext.WORKER_LINEAGE_SYNC, 5, TimeUnit.SECONDS));

      infos = lineageMasterClient.getLineageInfoList();
      Assert.assertEquals(LineageFileState.PERSISENCE_REQUESTED.toString(),
          infos.get(0).outputFiles.get(0).state);

      // sleep and wait for worker to persist the file
      Thread.sleep(5);

      // worker notifies the master
      HeartbeatScheduler.schedule(HeartbeatContext.WORKER_LINEAGE_SYNC);
      Assert.assertTrue(
          HeartbeatScheduler.await(HeartbeatContext.WORKER_LINEAGE_SYNC, 5, TimeUnit.SECONDS));

      infos = lineageMasterClient.getLineageInfoList();
      Assert.assertEquals(LineageFileState.PERSISTED.toString(),
          infos.get(0).outputFiles.get(0).state);

    } finally {
      lineageMasterClient.close();
    }
  }

  private LineageMasterClient getLineageMasterClient() {
    return new LineageMasterClient(mLocalTachyonClusterResource.get().getMaster().getAddress(),
        mTestConf);
  }
}
