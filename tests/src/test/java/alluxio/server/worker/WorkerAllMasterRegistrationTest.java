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
import alluxio.exception.runtime.UnavailableRuntimeException;
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

  private WaitForOptions mDefaultWaitForOptions = WaitForOptions.defaults().setTimeoutMs(30000);

  @Before
  public void before() throws Exception {
    mCluster = new MultiMasterEmbeddedJournalLocalAlluxioCluster(
        mNumMasters, mNumWorkers, PortCoordination.WORKER_ALL_MASTER_REGISTRATION);
    mCluster.initConfiguration(
        IntegrationTestUtils.getTestName(getClass().getSimpleName(), mTestName.getMethodName()));
    Configuration.set(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, 100);
    Configuration.set(PropertyKey.WORKER_REGISTER_TO_ALL_MASTERS, true);
    Configuration.set(PropertyKey.STANDBY_MASTER_GRPC_ENABLED, true);
    Configuration.set(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.MUST_CACHE);
    Configuration.set(PropertyKey.WORKER_BLOCK_HEARTBEAT_REPORT_SIZE_THRESHOLD, 5);
    Configuration.set(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "30sec");
    Configuration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_WRITE_TIMEOUT, "10sec");
    Configuration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "3s");
    Configuration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "6s");

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

  /**
   * Tests a happy path where added and removed blocks can all be reported to standby masters.
   */
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

    // New blocks are added by committing journals
    CommonUtils.waitFor("wait for blocks being committed to all masters", () ->
        mBlockMasters.stream().allMatch(it -> {
          try {
            return it.getBlockMetaStore().getLocations(blockId).size() == 1;
          } catch (UnavailableRuntimeException e) {
            // The RocksDB is unavailable due to events like checkpoint
            // Just retry
          }
          return false;
        }), mDefaultWaitForOptions);

    // Removed blocks are reported by worker-master heartbeats
    mWorker.removeBlock(new Random().nextLong(), blockId);
    CommonUtils.waitFor("wait for blocks being removed to all masters", () ->
        mBlockMasters.stream().allMatch(it -> {
          try {
            return it.getBlockMetaStore().getLocations(blockId).size() == 0;
          } catch (UnavailableRuntimeException e) {
            // The RocksDB is unavailable due to events like checkpoint
            // Just retry
          }
          return false;
        }), mDefaultWaitForOptions);

    assertTrue(mWorker.getBlockSyncMasterGroup().isRegisteredToAllMasters());

    fis = mCluster.getClient().openFile(fileUri);
    FileInStream finalFis = fis;

    // Make sure registration only happen once to each master
    assertTrue(getBlockSyncOperators().values().stream()
        .allMatch(it -> it.getRegistrationSuccessCount() == 1));

    assertThrows(UnavailableException.class,
        () -> IOUtils.toString(finalFis, Charset.defaultCharset()));
  }

  /**
   * Tests a scenario where the worker to master heartbeat fails.
   */
  @Test
  public void workerHeartbeatFail() throws Exception {
    CommonUtils.waitFor("wait for worker registration complete", () ->
        mWorker.getBlockSyncMasterGroup().isRegisteredToAllMasters(), mDefaultWaitForOptions);

    // Fails all heartbeats
    getBlockSyncOperators().values().forEach(TestSpecificMasterBlockSync::failHeartbeat);

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

    // Added blocks are replicated to standbys by journals,
    // so even if the heartbeat fails, standby are still in sync with primary.
    CommonUtils.waitFor("wait for blocks being committed to all masters by heartbeats",
        () ->
        mBlockMasters.stream().allMatch(it -> {
          try {
            return it.getBlockMetaStore().getLocations(blockId).size() == 1;
          } catch (UnavailableRuntimeException e) {
            // The RocksDB is unavailable due to events like checkpoint
            // Just retry
          }
          return false;
        }), mDefaultWaitForOptions);

    // Remove a block
    mWorker.removeBlock(new Random().nextLong(), blockId);

    // Resume all heartbeats and the block location should be moved on standby masters,
    // by heartbeats
    getBlockSyncOperators().values().forEach(TestSpecificMasterBlockSync::restoreHeartbeat);
    CommonUtils.waitFor("wait for blocks being removed on all masters by heartbeats",
        () ->
            mBlockMasters.stream().allMatch(it -> {
              try {
                return it.getBlockMetaStore().getLocations(blockId).size() == 0;
              } catch (UnavailableRuntimeException e) {
                // The RocksDB is unavailable due to events like checkpoint
                // Just retry
              }
              return false;
            }), mDefaultWaitForOptions);

    // Make sure registration only happen once to each master
    assertTrue(getBlockSyncOperators().values().stream()
        .allMatch(it -> it.getRegistrationSuccessCount() == 1));
  }

  /**
   * Tests the master failover case and makes sure the re-registration does not happen,
   * on the new elected primary master.
   */
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
    mCluster.waitForPrimaryMasterServing(10000);
    assertNotEquals(mCluster.getLeaderIndex(), leaderId);

    fis = mCluster.getClient().openFile(fileUri);
    FileInStream finalFis = fis;
    // The new elected primary master should be able to serve the request immediately,
    // because the added block location is replicated by journal.
    assertEquals(fileContent, IOUtils.toString(finalFis, Charset.defaultCharset()));

    // Make sure no more registration happens
    assertTrue(getBlockSyncOperators().values().stream()
        .allMatch(it -> it.getRegistrationSuccessCount() == 1));
  }

  /**
   * Tests worker being able to worker and re-register to all masters after its restart.
   */
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

  /**
   * Tests the worker can re-register with masters if its heartbeat failed too many times,
   * and the block report becomes too big and takes up too many memory.
   */
  @Test
  public void heartbeatFallsBackToRegister() throws Exception {
    CommonUtils.waitFor("wait for worker registration complete", () ->
        mWorker.getBlockSyncMasterGroup().isRegisteredToAllMasters(), mDefaultWaitForOptions);

    // Create a test file whose block will be removed later
    AlluxioURI testFileUri = new AlluxioURI("/foo");
    FileOutStream fos = mCluster.getLocalAlluxioMaster().getClient().createFile(
        new AlluxioURI("/foo"));
    fos.write("foo".getBytes());
    fos.close();
    List<BlockLocationInfo> blockLocations =
        mCluster.getClient().getBlockLocations(testFileUri);
    long testFileBlockId = blockLocations.get(0).getBlockInfo().getBlockInfo().getBlockId();

    List<Long> blockIdsToRemove = new ArrayList<>();
    int numFiles = 10;
    // Create 10 files and corresponding 10 blocks.
    // These blocks are added to standby masters by journals.
    for (int i = 0; i < numFiles; ++i) {
      AlluxioURI fileUri = new AlluxioURI("/" + i);
      fos = mCluster.getLocalAlluxioMaster().getClient().createFile(
          fileUri);
      fos.write("foo".getBytes());
      fos.close();
      blockLocations = mCluster.getClient().getBlockLocations(fileUri);
      long blockId = blockLocations.get(0).getBlockInfo().getBlockInfo().getBlockId();
      blockIdsToRemove.add(blockId);
    }

    // Make heartbeat return fail
    getBlockSyncOperators().values().forEach(TestSpecificMasterBlockSync::failHeartbeat);

    // Removing the blocks for these 10 files,
    // heartbeat report containing these blocks will be generated during the heartbeat.
    // However, the heartbeat RPC to master will not succeed because
    // we made the heartbeat fail.
    // So the heartbeat report will become larger and larger as we merge the report
    // back to the reporter on RPC failures.
    Thread fileGenerationThread = new Thread(() -> {
      for (long blockId: blockIdsToRemove) {
        try {
          mWorker.removeBlock(0, blockId);
          Thread.sleep(500);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
    fileGenerationThread.start();

    // Registration should trigger after heartbeat throws exceptions,
    // because the heartbeat report contains too many block ids and exceeds the
    // WORKER_BLOCK_HEARTBEAT_REPORT_CAPACITY_THRESHOLD and
    // the worker will trigger re-registration.
    CommonUtils.waitFor("wait for re-registration sending the block location info",
        () -> getBlockSyncOperators().values().stream()
            .allMatch(it -> it.getRegistrationSuccessCount() >= 2),
        WaitForOptions.defaults().setTimeoutMs(60000));

    // Remove one block and resume the heartbeats,
    // and make sure all blocks are propagated to all masters after heartbeat resumes;
    fileGenerationThread.join();
    getBlockSyncOperators().values().forEach(TestSpecificMasterBlockSync::restoreHeartbeat);
    mWorker.removeBlock(1, testFileBlockId);

    // We have removed the test block id + 10 other block ids,
    // so the block meta store should not contain any block location
    blockIdsToRemove.add(testFileBlockId);
    CommonUtils.waitFor("wait for blocks propagated to masters by heartbeats",
        () -> mBlockMasters.stream().allMatch(it ->
                blockIdsToRemove.stream()
                    .allMatch(blockId -> {
                      try {
                        return it.getBlockMetaStore().getLocations(blockId).size() == 0;
                      } catch (UnavailableRuntimeException e) {
                        // The RocksDB is unavailable due to events like checkpoint
                        // Just retry
                      }
                      return false;
                    })
            ),
        mDefaultWaitForOptions);
  }

  private Map<InetSocketAddress, TestSpecificMasterBlockSync> getBlockSyncOperators() {
    return Maps.transformValues(mWorker.getBlockSyncMasterGroup().getMasterSyncOperators(),
        it -> (TestSpecificMasterBlockSync) it);
  }
}
