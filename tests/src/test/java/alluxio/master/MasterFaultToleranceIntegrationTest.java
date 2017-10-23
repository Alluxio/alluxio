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

package alluxio.master;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.BaseIntegrationTest;
import alluxio.client.WriteType;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.collections.Pair;
import alluxio.exception.AlluxioException;
import alluxio.hadoop.HadoopClientTestUtils;
import alluxio.master.block.BlockMaster;
import alluxio.thrift.CommandType;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.PathUtils;

import com.google.common.base.Function;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Ignore("https://alluxio.atlassian.net/browse/ALLUXIO-2818")
public class MasterFaultToleranceIntegrationTest extends BaseIntegrationTest {
  // Fail if the cluster doesn't come up after this amount of time.
  private static final int CLUSTER_WAIT_TIMEOUT_MS = 120 * Constants.SECOND_MS;
  private static final long WORKER_CAPACITY_BYTES = 10000;
  private static final int BLOCK_SIZE = 30;
  private static final int MASTERS = 5;

  private MultiMasterLocalAlluxioCluster mMultiMasterLocalAlluxioCluster = null;
  private FileSystem mFileSystem = null;

  @BeforeClass
  public static void beforeClass() {
    // Skip hadoop 1 because hadoop 1's RPC cannot be interrupted properly which makes it
    // hard to shutdown a cluster.
    // TODO(peis): Figure out a better way to support hadoop 1.
    Assume.assumeFalse(HadoopClientTestUtils.isHadoop1x());
  }

  @After
  public final void after() throws Exception {
    mMultiMasterLocalAlluxioCluster.stop();
  }

  @Before
  public final void before() throws Exception {
    // TODO(gpang): Implement multi-master cluster as a resource.
    mMultiMasterLocalAlluxioCluster =
        new MultiMasterLocalAlluxioCluster(MASTERS);
    mMultiMasterLocalAlluxioCluster.initConfiguration();
    Configuration.set(PropertyKey.WORKER_MEMORY_SIZE, WORKER_CAPACITY_BYTES);
    Configuration.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE);
    Configuration.set(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS, 100);
    Configuration.set(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES, 2);
    Configuration.set(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, 32);
    mMultiMasterLocalAlluxioCluster.start();
    mFileSystem = mMultiMasterLocalAlluxioCluster.getClient();
  }

  /**
   * Creates 10 files in the folder.
   *
   * @param folderName the folder name to create
   * @param answer the results, the mapping from file id to file path
   */
  private void faultTestDataCreation(AlluxioURI folderName, List<Pair<Long, AlluxioURI>> answer)
      throws IOException, AlluxioException {
    mFileSystem.createDirectory(folderName);
    answer.add(new Pair<>(mFileSystem.getStatus(folderName).getFileId(), folderName));

    for (int k = 0; k < 10; k++) {
      AlluxioURI path =
          new AlluxioURI(PathUtils.concatPath(folderName, folderName.toString().substring(1) + k));
      mFileSystem.createFile(path).close();
      answer.add(new Pair<>(mFileSystem.getStatus(path).getFileId(), path));
    }
  }

  /**
   * Tells if the results can match the answers.
   *
   * @param answers the correct results
   */
  private void faultTestDataCheck(List<Pair<Long, AlluxioURI>> answers) throws IOException,
      AlluxioException {
    List<String> files = FileSystemTestUtils.listFiles(mFileSystem, AlluxioURI.SEPARATOR);
    Collections.sort(files);
    Assert.assertEquals(answers.size(), files.size());
    for (Pair<Long, AlluxioURI> answer : answers) {
      Assert.assertEquals(answer.getSecond().toString(),
          mFileSystem.getStatus(answer.getSecond()).getPath());
      Assert.assertEquals(answer.getFirst().longValue(),
          mFileSystem.getStatus(answer.getSecond()).getFileId());
    }
  }

  /**
   * Wait for a number of workers to register. This call will block until the block master
   * detects the required number of workers or if the timeout is exceeded.
   *
   * @param store the block store object which references the correct block master
   * @param numWorkers the number of workers to wait for
   * @param timeoutMs the number of milliseconds to wait before timing out
   */
  private void waitForWorkerRegistration(final AlluxioBlockStore store, final int numWorkers,
      int timeoutMs) {
    CommonUtils.waitFor("Worker to register.", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void aVoid) {
        try {
          return store.getWorkerInfoList().size() >= numWorkers;
        } catch (Exception e) {
          return false;
        }
      }
    }, WaitForOptions.defaults().setTimeoutMs(timeoutMs));
  }

  @Test
  public void createFileFault() throws Exception {
    int clients = 10;
    List<Pair<Long, AlluxioURI>> answer = new ArrayList<>();
    for (int k = 0; k < clients; k++) {
      faultTestDataCreation(new AlluxioURI("/data" + k), answer);
    }
    faultTestDataCheck(answer);

    for (int kills = 0; kills < MASTERS - 1; kills++) {
      Assert.assertTrue(mMultiMasterLocalAlluxioCluster.stopLeader());
      mMultiMasterLocalAlluxioCluster.waitForNewMaster(CLUSTER_WAIT_TIMEOUT_MS);
      waitForWorkerRegistration(AlluxioBlockStore.create(), 1, CLUSTER_WAIT_TIMEOUT_MS);
      faultTestDataCheck(answer);
      faultTestDataCreation(new AlluxioURI("/data_kills_" + kills), answer);
    }
  }

  @Test
  public void deleteFileFault() throws Exception {
    // Kill leader -> create files -> kill leader -> delete files, repeat.
    List<Pair<Long, AlluxioURI>> answer = new ArrayList<>();
    for (int kills = 0; kills < MASTERS - 1; kills++) {
      Assert.assertTrue(mMultiMasterLocalAlluxioCluster.stopLeader());
      mMultiMasterLocalAlluxioCluster.waitForNewMaster(CLUSTER_WAIT_TIMEOUT_MS);
      waitForWorkerRegistration(AlluxioBlockStore.create(), 1, CLUSTER_WAIT_TIMEOUT_MS);

      if (kills % 2 != 0) {
        // Delete files.

        faultTestDataCheck(answer);

        // We can not call mFileSystem.delete(mFileSystem.open(new
        // AlluxioURI(AlluxioURI.SEPARATOR))) because root node can not be deleted.
        for (URIStatus file : mFileSystem.listStatus(new AlluxioURI(AlluxioURI.SEPARATOR))) {
          mFileSystem.delete(new AlluxioURI(file.getPath()),
              DeleteOptions.defaults().setRecursive(true));
        }
        answer.clear();
        faultTestDataCheck(answer);
      } else {
        // Create files.

        Assert.assertEquals(0, answer.size());
        faultTestDataCheck(answer);

        faultTestDataCreation(new AlluxioURI(PathUtils.concatPath(
            AlluxioURI.SEPARATOR, "data_" + kills)), answer);
        faultTestDataCheck(answer);
      }
    }
  }

  @Test
  public void createFiles() throws Exception {
    int clients = 10;
    CreateFileOptions option =
        CreateFileOptions.defaults().setBlockSizeBytes(1024).setWriteType(WriteType.THROUGH);
    for (int k = 0; k < clients; k++) {
      mFileSystem.createFile(new AlluxioURI(AlluxioURI.SEPARATOR + k), option).close();
    }
    List<String> files = FileSystemTestUtils.listFiles(mFileSystem, AlluxioURI.SEPARATOR);
    Assert.assertEquals(clients, files.size());
    Collections.sort(files);
    for (int k = 0; k < clients; k++) {
      Assert.assertEquals(AlluxioURI.SEPARATOR + k, files.get(k));
    }
  }

  @Test
  public void killStandby() throws Exception {
    // If standby masters are killed(or node failure), current leader should not be affected and the
    // cluster should run properly.

    int leaderIndex = mMultiMasterLocalAlluxioCluster.getLeaderIndex();
    Assert.assertNotEquals(-1, leaderIndex);

    List<Pair<Long, AlluxioURI>> answer = new ArrayList<>();
    for (int k = 0; k < 5; k++) {
      faultTestDataCreation(new AlluxioURI("/data" + k), answer);
    }
    faultTestDataCheck(answer);

    for (int kills = 0; kills < MASTERS - 1; kills++) {
      Assert.assertTrue(mMultiMasterLocalAlluxioCluster.stopStandby());
      CommonUtils.sleepMs(Constants.SECOND_MS * 2);

      // Leader should not change.
      Assert.assertEquals(leaderIndex, mMultiMasterLocalAlluxioCluster.getLeaderIndex());
      // Cluster should still work.
      faultTestDataCheck(answer);
      faultTestDataCreation(new AlluxioURI("/data_kills_" + kills), answer);
    }
  }

  @Test
  public void workerReRegister() throws Exception {
    AlluxioBlockStore store = AlluxioBlockStore.create();
    Assert.assertEquals(WORKER_CAPACITY_BYTES, store.getCapacityBytes());

    for (int kills = 0; kills < MASTERS - 1; kills++) {
      Assert.assertTrue(mMultiMasterLocalAlluxioCluster.stopLeader());
      mMultiMasterLocalAlluxioCluster.waitForNewMaster(CLUSTER_WAIT_TIMEOUT_MS);
      waitForWorkerRegistration(store, 1, 1 * Constants.MINUTE_MS);
      // If worker is successfully re-registered, the capacity bytes should not change.
      long capacityFound = store.getCapacityBytes();
      Assert.assertEquals(WORKER_CAPACITY_BYTES, capacityFound);
    }
  }

  @Test
  public void failoverWorkerRegister() throws Exception {
    // Stop the default cluster.
    after();

    // Create a new cluster, with no workers initially
    final MultiMasterLocalAlluxioCluster cluster = new MultiMasterLocalAlluxioCluster(2, 0);
    cluster.initConfiguration();
    cluster.start();
    try {
      // Get the first block master
      BlockMaster blockMaster1 =
          cluster.getLocalAlluxioMaster().getMasterProcess().getMaster(BlockMaster.class);
      // Register worker 1
      long workerId1a =
          blockMaster1.getWorkerId(new alluxio.wire.WorkerNetAddress().setHost("host1"));
      blockMaster1.workerRegister(workerId1a, Collections.EMPTY_LIST, Collections.EMPTY_MAP,
          Collections.EMPTY_MAP, Collections.EMPTY_MAP);

      // Register worker 2
      long workerId2a =
          blockMaster1.getWorkerId(new alluxio.wire.WorkerNetAddress().setHost("host2"));
      blockMaster1.workerRegister(workerId2a, Collections.EMPTY_LIST, Collections.EMPTY_MAP,
          Collections.EMPTY_MAP, Collections.EMPTY_MAP);

      Assert.assertEquals(2, blockMaster1.getWorkerCount());
      // Worker heartbeats should return "Nothing"
      Assert.assertEquals(CommandType.Nothing, blockMaster1
          .workerHeartbeat(workerId1a, Collections.EMPTY_MAP, Collections.EMPTY_LIST,
              Collections.EMPTY_MAP).getCommandType());
      Assert.assertEquals(CommandType.Nothing, blockMaster1
          .workerHeartbeat(workerId2a, Collections.EMPTY_MAP, Collections.EMPTY_LIST,
              Collections.EMPTY_MAP).getCommandType());

      Assert.assertTrue(cluster.stopLeader());
      cluster.waitForNewMaster(CLUSTER_WAIT_TIMEOUT_MS);

      // Get the new block master, after the failover
      BlockMaster blockMaster2 = cluster.getLocalAlluxioMaster().getMasterProcess()
          .getMaster(BlockMaster.class);

      // Worker 2 tries to heartbeat (with original id), and should get "Register" in response.
      Assert.assertEquals(CommandType.Register, blockMaster2
          .workerHeartbeat(workerId2a, Collections.EMPTY_MAP, Collections.EMPTY_LIST,
              Collections.EMPTY_MAP).getCommandType());

      // Worker 2 re-registers (and gets a new worker id)
      long workerId2b =
          blockMaster2.getWorkerId(new alluxio.wire.WorkerNetAddress().setHost("host2"));
      blockMaster2.workerRegister(workerId2b, Collections.EMPTY_LIST, Collections.EMPTY_MAP,
          Collections.EMPTY_MAP, Collections.EMPTY_MAP);

      // Worker 1 tries to heartbeat (with original id), and should get "Register" in response.
      Assert.assertEquals(CommandType.Register, blockMaster2
          .workerHeartbeat(workerId1a, Collections.EMPTY_MAP, Collections.EMPTY_LIST,
              Collections.EMPTY_MAP).getCommandType());

      // Worker 1 re-registers (and gets a new worker id)
      long workerId1b =
          blockMaster2.getWorkerId(new alluxio.wire.WorkerNetAddress().setHost("host1"));
      blockMaster2.workerRegister(workerId1b, Collections.EMPTY_LIST, Collections.EMPTY_MAP,
          Collections.EMPTY_MAP, Collections.EMPTY_MAP);

    } finally {
      cluster.stop();
    }

    // Start the default cluster.
    before();
  }
}
