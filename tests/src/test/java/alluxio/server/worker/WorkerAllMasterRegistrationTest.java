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

package alluxio.server.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.UnavailableException;
import alluxio.master.MultiMasterEmbeddedJournalLocalAlluxioCluster;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.DefaultBlockMaster;
import alluxio.multi.process.PortCoordination;
import alluxio.testutils.IntegrationTestUtils;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.wire.BlockLocationInfo;
import alluxio.worker.block.AllMasterRegistrationBlockWorker;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.SpecificMasterBlockSync;
import alluxio.worker.block.TestSpecificMasterBlockSync;

import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class WorkerAllMasterRegistrationTest {
  private MultiMasterEmbeddedJournalLocalAlluxioCluster mCluster;

  @Rule
  public TestName mTestName = new TestName();

  private final List<DefaultBlockMaster> mBlockMasters = new ArrayList<>();
  private AllMasterRegistrationBlockWorker mWorker;
  private final int mNumMasters = 3;
  private final int mNumWorkers = 1;

  private WaitForOptions mDefaultWaitForOptions = WaitForOptions.defaults().setTimeoutMs(20000);

  @Before
  public void before() throws Exception {
    mCluster = new MultiMasterEmbeddedJournalLocalAlluxioCluster(
        mNumMasters, mNumWorkers, PortCoordination.WORKER_ALL_MASTER_REGISTRATION);
    mCluster.initConfiguration(
        IntegrationTestUtils.getTestName(getClass().getSimpleName(), mTestName.getMethodName()));
    Configuration.set(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES, 5);
    Configuration.set(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, 100);
    Configuration.set(PropertyKey.WORKER_REGISTER_TO_ALL_MASTERS, true);
    Configuration.set(PropertyKey.STANDBY_MASTER_GRPC_ENABLED, true);
    Configuration.set(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.MUST_CACHE);
    mCluster.start();

    mWorker = (AllMasterRegistrationBlockWorker)
        mCluster.getWorkerProcess(0).getWorker(BlockWorker.class);
    for (int i = 0; i < mNumMasters; i++) {
      mBlockMasters.add((DefaultBlockMaster)
          mCluster.getLocalAlluxioMasterByIndex(i).getMasterProcess().getMaster(BlockMaster.class));
    }
  }

  @After
  public void after() throws Exception {
    mCluster.stop();
    mWorker.stop();
    mWorker = null;
    mBlockMasters.clear();
  }

  @Test
  public void happyPath() throws Exception {
    CommonUtils.waitFor("wait for worker registration complete", () ->
        mWorker.getBlockSyncMasterGroup().isRegisteredToAllMasters(), mDefaultWaitForOptions);

    AlluxioURI fileUri = new AlluxioURI("/foobar");
    String fileContent = "foobar";

    FileOutStream fos = mCluster.getClient().createFile(fileUri);
    fos.write(fileContent.getBytes());
    fos.close();

    FileInStream fis = mCluster.getClient().openFile(fileUri);
    assertEquals(fileContent, IOUtils.toString(fis, Charset.defaultCharset()));

    List<BlockLocationInfo> blockLocations =
        mCluster.getClient().getBlockLocations(fileUri);
    assertEquals(1, blockLocations.size());
    assertEquals(1, blockLocations.get(0).getLocations().size());
    long blockId = blockLocations.get(0).getBlockInfo().getBlockInfo().getBlockId();

    CommonUtils.waitFor("wait for blocks being committed to all masters", () ->
        mBlockMasters.stream().allMatch(
            it -> it.getBlockMetaStore().getLocations(blockId).size() == 1),
        mDefaultWaitForOptions);

    mWorker.removeBlock(new Random().nextLong(), blockId);
    CommonUtils.waitFor("wait for blocks being removed to all masters", () ->
        mBlockMasters.stream().allMatch(
            it -> it.getBlockMetaStore().getLocations(blockId).size() == 0),
        mDefaultWaitForOptions);

    assertTrue(mWorker.getBlockSyncMasterGroup().isRegisteredToAllMasters());

    fis = mCluster.getClient().openFile(fileUri);
    FileInStream finalFis = fis;

    // Make sure registration only happen once to each master
    assertTrue(getBlockSyncOperators().values().stream()
        .allMatch(it -> it.getRegistrationSuccessCount() == 1));

    assertThrows(UnavailableException.class,
        () -> IOUtils.toString(finalFis, Charset.defaultCharset()));
  }

  @Test
  public void workerHeartbeatPaused() throws Exception {
    CommonUtils.waitFor("wait for worker registration complete", () ->
        mWorker.getBlockSyncMasterGroup().isRegisteredToAllMasters(), mDefaultWaitForOptions);

    // Pause all heartbeats
    getBlockSyncOperators().values().forEach(TestSpecificMasterBlockSync::pauseHeartbeat);

    // Write a file
    AlluxioURI fileUri = new AlluxioURI("/foobar");
    String fileContent = "foobar";

    FileOutStream fos = mCluster.getClient().createFile(fileUri);
    fos.write(fileContent.getBytes());
    fos.close();

    // Committed block is on primary master even if the heartbeat is paused.
    List<BlockLocationInfo> blockLocations =
        mCluster.getClient().getBlockLocations(fileUri);
    assertEquals(1, blockLocations.size());
    assertEquals(1, blockLocations.get(0).getLocations().size());
    long blockId = blockLocations.get(0).getBlockInfo().getBlockInfo().getBlockId();

    // Resume all heartbeats and the block location should be sent to standby masters
    getBlockSyncOperators().values().forEach(TestSpecificMasterBlockSync::resumeHeartbeat);

    CommonUtils.waitFor("wait for blocks being committed to all masters by heartbeats",
        () ->
        mBlockMasters.stream().allMatch(
            it -> it.getBlockMetaStore().getLocations(blockId).size() == 1),
        mDefaultWaitForOptions);

    // Make sure registration only happen once to each master
    assertTrue(getBlockSyncOperators().values().stream()
        .allMatch(it -> it.getRegistrationSuccessCount() == 1));
  }

  @Test
  public void masterFailover() throws Exception {
    CommonUtils.waitFor("wait for worker registration complete", () ->
        mWorker.getBlockSyncMasterGroup().isRegisteredToAllMasters(), mDefaultWaitForOptions);

    AlluxioURI fileUri = new AlluxioURI("/foobar");
    String fileContent = "foobar";

    FileOutStream fos = mCluster.getClient().createFile(fileUri);
    fos.write(fileContent.getBytes());
    fos.close();

    FileInStream fis = mCluster.getClient().openFile(fileUri);
    assertEquals(fileContent, IOUtils.toString(fis, Charset.defaultCharset()));

    // Make sure registration only happen once to each master
    assertTrue(getBlockSyncOperators().values().stream()
        .allMatch(it -> it.getRegistrationSuccessCount() == 1));

    // Kill the master and let the failover happen
    int leaderId = mCluster.getLeaderIndex();
    mCluster.stopLeader();
    mCluster.waitForPrimaryMasterServing(5000);
    assertNotEquals(mCluster.getLeaderIndex(), leaderId);

    fis = mCluster.getClient().openFile(fileUri);
    FileInStream finalFis = fis;
    CommonUtils.waitFor("wait for heartbeat sending the block location info",
        () -> {
          try {
            return fileContent.equals(IOUtils.toString(finalFis, Charset.defaultCharset()));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        },
        mDefaultWaitForOptions);

    // Make sure no more registration happens
    assertTrue(getBlockSyncOperators().values().stream()
        .allMatch(it -> it.getRegistrationSuccessCount() == 1));
  }

  @Test
  public void workerRestart() throws Exception {
    CommonUtils.waitFor("wait for worker registration complete", () ->
        mWorker.getBlockSyncMasterGroup().isRegisteredToAllMasters(), mDefaultWaitForOptions);

    AlluxioURI fileUri = new AlluxioURI("/foobar");
    String fileContent = "foobar";

    FileOutStream fos = mCluster.getClient().createFile(fileUri);
    fos.write(fileContent.getBytes());
    fos.close();

    FileInStream fis = mCluster.getClient().openFile(fileUri);
    assertEquals(fileContent, IOUtils.toString(fis, Charset.defaultCharset()));

    mCluster.stopWorkers();
    mCluster.startWorkers();

    mWorker = (AllMasterRegistrationBlockWorker)
        mCluster.getWorkerProcess(0).getWorker(BlockWorker.class);

    CommonUtils.waitFor("wait for worker registration complete", () ->
        getBlockSyncOperators().values().stream().allMatch(
            SpecificMasterBlockSync::isRegistered), mDefaultWaitForOptions);

    fis = mCluster.getClient().openFile(fileUri);
    assertEquals(fileContent, IOUtils.toString(fis, Charset.defaultCharset()));
  }

  private Map<InetSocketAddress, TestSpecificMasterBlockSync> getBlockSyncOperators() {
    return Maps.transformValues(mWorker.getBlockSyncMasterGroup().getMasterSyncOperators(),
        it -> (TestSpecificMasterBlockSync) it);
  }
}
