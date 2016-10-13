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

package alluxio;

import alluxio.client.file.FileSystemMasterClient;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.util.CommonUtils;
import alluxio.worker.block.BlockHeartbeatReporter;
import alluxio.worker.block.BlockWorker;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import org.powermock.reflect.Whitebox;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

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
      AlluxioURI uri) {
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

    try (FileSystemMasterClient client = new FileSystemMasterClient(
        localAlluxioClusterResource.get().getMaster().getAddress())) {
      CommonUtils.waitFor(uri + " to be persisted", new Function<Void, Boolean>() {
        @Override
        public Boolean apply(Void input) {
          try {
            return client.getStatus(uri).isPersisted();
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      }, timeoutMs);
    }
  }

  /**
   * @return a random sequence of characters from 'a' to 'z' of random length up to 100 characters
   */
  public static String randomString() {
    Random random = new Random();
    int length = random.nextInt(100) + 1;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      sb.append((char) (random.nextInt(26) + 97));
    }
    return sb.toString();
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
      }, 100 * Constants.SECOND_MS);

      // Execute 2nd heartbeat from worker.
      HeartbeatScheduler.execute(HeartbeatContext.WORKER_BLOCK_SYNC);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private IntegrationTestUtils() {} // This is a utils class not intended for instantiation
}
