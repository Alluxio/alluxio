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

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.options.GetStatusOptions;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
<<<<<<< HEAD
import alluxio.master.MasterClientConfig;
||||||| parent of 14dc0bd30b... Fix secondary master to create checkpoints (#8391)
import alluxio.master.MasterClientContext;
import alluxio.master.PortRegistry;
=======
import alluxio.master.MasterClientContext;
import alluxio.master.NoopMaster;
import alluxio.master.PortRegistry;
import alluxio.master.journal.JournalUtils;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.master.journal.ufs.UfsJournalSnapshot;
>>>>>>> 14dc0bd30b... Fix secondary master to create checkpoints (#8391)
import alluxio.util.CommonUtils;
import alluxio.util.URIUtils;
import alluxio.util.WaitForOptions;
import alluxio.worker.block.BlockHeartbeatReporter;
import alluxio.worker.block.BlockWorker;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
      AlluxioURI uri) throws IOException {
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
      final AlluxioURI uri, int timeoutMs) {
    try (FileSystemMasterClient client =
        FileSystemMasterClient.Factory.create(MasterClientConfig.defaults())) {
      CommonUtils.waitFor(uri + " to be persisted", new Function<Void, Boolean>() {
        @Override
        public Boolean apply(Void input) {
          try {
            return client.getStatus(uri, GetStatusOptions.defaults()).isPersisted();
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
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
      int timeoutMs) {
    CommonUtils.waitFor(uri + " to be cached", (input) -> {
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
  public static void waitForBlocksToBeFreed(final BlockWorker bw, final Long... blockIds) {
    try {
      // Execute 1st heartbeat from worker.
      HeartbeatScheduler.execute(HeartbeatContext.WORKER_BLOCK_SYNC);

      // Waiting for the blocks to be added into the heartbeat reportor, so that they will be
      // removed from master in the next heartbeat.
      CommonUtils.waitFor("blocks to be removed", new Function<Void, Boolean>() {
        @Override
        public Boolean apply(Void input) {
          BlockHeartbeatReporter reporter = Whitebox.getInternalState(bw, "mHeartbeatReporter");
          List<Long> blocksToRemove = Whitebox.getInternalState(reporter, "mRemovedBlocks");
          return blocksToRemove.containsAll(Arrays.asList(blockIds));
        }
      }, WaitForOptions.defaults().setTimeoutMs(100 * Constants.SECOND_MS));

      // Execute 2nd heartbeat from worker.
      HeartbeatScheduler.execute(HeartbeatContext.WORKER_BLOCK_SYNC);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

<<<<<<< HEAD
||||||| parent of 14dc0bd30b... Fix secondary master to create checkpoints (#8391)
  /**
   * Reserves ports for each master service and updates the server configuration.
   */
  public static void reserveMasterPorts() {
    for (ServiceType service : Arrays.asList(ServiceType.MASTER_RPC, ServiceType.MASTER_WEB,
        ServiceType.JOB_MASTER_RPC, ServiceType.JOB_MASTER_WEB)) {
      PropertyKey key = service.getPortKey();
      ServerConfiguration.set(key, PortRegistry.reservePort());
    }
  }

  public static void releaseMasterPorts() {
    PortRegistry.clear();
  }

=======
  /**
   * Waits for a checkpoint to be written in the specified master's journal.
   *
   * @param masterName the name of the master
   */
  public static void waitForCheckpoint(String masterName)
      throws TimeoutException, InterruptedException {
    UfsJournal journal = new UfsJournal(URIUtils.appendPathOrDie(JournalUtils.getJournalLocation(),
        masterName), new NoopMaster(""), 0);
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
        ServiceType.JOB_MASTER_RPC, ServiceType.JOB_MASTER_WEB)) {
      PropertyKey key = service.getPortKey();
      ServerConfiguration.set(key, PortRegistry.reservePort());
    }
  }

  public static void releaseMasterPorts() {
    PortRegistry.clear();
  }

>>>>>>> 14dc0bd30b... Fix secondary master to create checkpoints (#8391)
  private IntegrationTestUtils() {} // This is a utils class not intended for instantiation
}
