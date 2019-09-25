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

package alluxio.testutils;

import static alluxio.util.network.NetworkAddressUtils.ServiceType;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.master.MasterClientContext;
import alluxio.master.NoopMaster;
import alluxio.master.PortRegistry;
import alluxio.master.journal.JournalUtils;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.master.journal.ufs.UfsJournalSnapshot;
import alluxio.util.CommonUtils;
import alluxio.util.FileSystemOptions;
import alluxio.util.URIUtils;
import alluxio.util.WaitForOptions;
import alluxio.worker.block.BlockHeartbeatReporter;
import alluxio.worker.block.BlockWorker;

import com.google.common.base.Throwables;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Util methods for writing integration tests.
 */
public final class IntegrationTestUtils {

  /**
   * Convenience method for calling
   * {@link #waitForPersist(LocalAlluxioClusterResource, AlluxioURI, int)} with a default timeout.
   *
   * @param localAlluxioClusterResource the cluster for the worker that will persist the file
   * @param uri the file uri to wait to be persisted
   */
  public static void waitForPersist(LocalAlluxioClusterResource localAlluxioClusterResource,
      AlluxioURI uri) throws TimeoutException, InterruptedException {
    waitForPersist(localAlluxioClusterResource, uri, 15 * Constants.SECOND_MS);
  }

  /**
   * Blocks until the specified file is persisted or a timeout occurs.
   *
   * @param localAlluxioClusterResource the cluster for the worker that will persist the file
   * @param uri the uri to wait to be persisted
   * @param timeoutMs the number of milliseconds to wait before giving up and throwing an exception
   */
  public static void waitForPersist(final LocalAlluxioClusterResource localAlluxioClusterResource,
      final AlluxioURI uri, int timeoutMs) throws InterruptedException, TimeoutException {
    try (FileSystemMasterClient client =
        FileSystemMasterClient.Factory.create(MasterClientContext
            .newBuilder(ClientContext.create(ServerConfiguration.global())).build())) {
      CommonUtils.waitFor(uri + " to be persisted", () -> {
        try {
          return client.getStatus(uri,
              FileSystemOptions.getStatusDefaults(ServerConfiguration.global())).isPersisted();
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }, WaitForOptions.defaults().setTimeoutMs(timeoutMs));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Blocks until the specified file is full cached in Alluxio or a timeout occurs.
   *
   * @param fileSystem the filesystem client
   * @param uri the uri to wait to be persisted
   * @param timeoutMs the number of milliseconds to wait before giving up and throwing an exception
   */
  public static void waitForFileCached(final FileSystem fileSystem, final AlluxioURI uri,
      int timeoutMs) throws TimeoutException, InterruptedException {
    CommonUtils.waitFor(uri + " to be cached", () -> {
      try {
        return fileSystem.getStatus(uri).getInAlluxioPercentage() == 100;
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }, WaitForOptions.defaults().setTimeoutMs(timeoutMs));
  }

  /**
   * Triggers two heartbeats to wait for a given list of blocks to be removed from both master and
   * worker.
   * Blocks until the master and block are in sync with the state of the blocks.
   *
   * @param bw the block worker that will remove the blocks
   * @param blockIds a list of blockIds to be removed
   */
  public static void waitForBlocksToBeFreed(final BlockWorker bw, final Long... blockIds)
      throws TimeoutException {
    try {
      // Execute 1st heartbeat from worker.
      HeartbeatScheduler.execute(HeartbeatContext.WORKER_BLOCK_SYNC);

      // Waiting for the blocks to be added into the heartbeat reportor, so that they will be
      // removed from master in the next heartbeat.
      CommonUtils.waitFor("blocks to be removed", () -> {
        BlockHeartbeatReporter reporter = Whitebox.getInternalState(bw, "mHeartbeatReporter");
        List<Long> blocksToRemove = Whitebox.getInternalState(reporter, "mRemovedBlocks");
        return blocksToRemove.containsAll(Arrays.asList(blockIds));
      }, WaitForOptions.defaults().setTimeoutMs(100 * Constants.SECOND_MS));

      // Execute 2nd heartbeat from worker.
      HeartbeatScheduler.execute(HeartbeatContext.WORKER_BLOCK_SYNC);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  /**
   * Waits for a checkpoint to be written in the specified master's journal.
   *
   * @param masterName the name of the master
   */
  public static void waitForUfsJournalCheckpoint(String masterName)
      throws TimeoutException, InterruptedException {
    waitForUfsJournalCheckpoint(masterName, JournalUtils.getJournalLocation());
  }

  /**
   * Waits for a checkpoint to be written in the specified master's journal.
   *
   * @param masterName the name of the master
   * @param journalLocation the location of the journal
   */
  public static void waitForUfsJournalCheckpoint(String masterName, URI journalLocation)
      throws TimeoutException, InterruptedException {
    UfsJournal journal = new UfsJournal(URIUtils.appendPathOrDie(journalLocation,
        masterName), new NoopMaster(""), 0, Collections::emptySet);
    CommonUtils.waitFor("checkpoint to be written", () -> {
      UfsJournalSnapshot snapshot;
      try {
        snapshot = UfsJournalSnapshot.getSnapshot(journal);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return !snapshot.getCheckpoints().isEmpty();
    });
  }

  /**
   * Reserves ports for each master service and updates the server configuration.
   */
  public static void reserveMasterPorts() {
    for (ServiceType service : Arrays.asList(ServiceType.MASTER_RPC, ServiceType.MASTER_WEB,
        ServiceType.MASTER_RAFT, ServiceType.JOB_MASTER_RPC, ServiceType.JOB_MASTER_WEB,
        ServiceType.JOB_MASTER_RAFT)) {
      PropertyKey key = service.getPortKey();
      ServerConfiguration.set(key, PortRegistry.reservePort());
    }
  }

  public static void releaseMasterPorts() {
    PortRegistry.clear();
  }

  /**
   * @param className the test class name
   * @param methodName the test method name
   * @return the combined test name
   */
  public static String getTestName(String className, String methodName) {
    String testName = className + "-" + methodName;
    // cannot use these characters in the name/path: . [ ]
    testName = testName.replace(".", "-");
    testName = testName.replace("[", "-");
    testName = testName.replace("]", "");
    return testName;
  }

  private IntegrationTestUtils() {} // This is a utils class not intended for instantiation
}
