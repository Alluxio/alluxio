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

package alluxio.worker.block;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.FailedToAcquireRegisterLeaseException;
import alluxio.grpc.Command;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.Scope;
import alluxio.metrics.MetricsSystem;
import alluxio.retry.ExponentialTimeBoundedRetry;
import alluxio.retry.RetryPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * The helper class for block master sync related methods.
 */
public class BlockMasterSyncHelper {
  private static final Logger LOG = LoggerFactory.getLogger(BlockMasterSync.class);

  private static final long ACQUIRE_LEASE_WAIT_BASE_SLEEP_MS =
      Configuration.getMs(PropertyKey.WORKER_REGISTER_LEASE_RETRY_SLEEP_MIN);
  private static final long ACQUIRE_LEASE_WAIT_MAX_SLEEP_MS =
      Configuration.getMs(PropertyKey.WORKER_REGISTER_LEASE_RETRY_SLEEP_MAX);
  private static final long ACQUIRE_LEASE_WAIT_MAX_DURATION =
      Configuration.getMs(PropertyKey.WORKER_REGISTER_LEASE_RETRY_MAX_DURATION);

  private final BlockMasterClient mMasterClient;

  /**
   * constructs an instance of the helper class.
   * @param masterClient the block master client
   */
  public BlockMasterSyncHelper(BlockMasterClient masterClient) {
    mMasterClient = masterClient;
  }

  @FunctionalInterface
  interface MasterCommandHandler {
    void handle(Command command) throws ConnectionFailedException, IOException;
  }

  /**
   * Gets the default retry policy for acquiring a {@link alluxio.wire.RegisterLease}
   * from the BlockMaster.
   *
   * @return the policy to use
   */
  public static RetryPolicy getDefaultAcquireLeaseRetryPolicy() {
    return ExponentialTimeBoundedRetry.builder()
        .withMaxDuration(Duration.of(ACQUIRE_LEASE_WAIT_MAX_DURATION, ChronoUnit.MILLIS))
        .withInitialSleep(Duration.of(ACQUIRE_LEASE_WAIT_BASE_SLEEP_MS, ChronoUnit.MILLIS))
        .withMaxSleep(Duration.of(ACQUIRE_LEASE_WAIT_MAX_SLEEP_MS, ChronoUnit.MILLIS))
        .withSkipInitialSleep()
        .build();
  }

  /**
   * acquires a lease from the master before registration.
   * @param workerId the worker id
   * @param storeMeta the store meta
   */
  void tryAcquireLease(
      long workerId, BlockStoreMeta storeMeta)
      throws IOException, FailedToAcquireRegisterLeaseException {
    boolean leaseRequired = Configuration.getBoolean(PropertyKey.WORKER_REGISTER_LEASE_ENABLED);
    if (leaseRequired) {
      LOG.info("Acquiring a RegisterLease from the master before registering");
      mMasterClient.acquireRegisterLeaseWithBackoff(workerId,
          storeMeta.getNumberOfBlocks(),
          getDefaultAcquireLeaseRetryPolicy());
      LOG.info("Lease acquired");
    }
    // If the worker registers with master successfully, the lease will be recycled on the
    // master side. No need to manually request for recycle on the worker side.
  }

  /**
   * registers the worker to the master.
   * @param workerId the worker id
   * @param fullStoreMeta the full store meta contains the block id list
   */
  void registerToMaster(
      long workerId, BlockStoreMeta fullStoreMeta) throws IOException {
    List<ConfigProperty> configList =
        Configuration.getConfiguration(Scope.WORKER);

    boolean useStreaming = Configuration.getBoolean(PropertyKey.WORKER_REGISTER_STREAM_ENABLED);
    if (useStreaming) {
      mMasterClient.registerWithStream(workerId,
          fullStoreMeta.getStorageTierAssoc().getOrderedStorageAliases(),
          fullStoreMeta.getCapacityBytesOnTiers(),
          fullStoreMeta.getUsedBytesOnTiers(), fullStoreMeta.getBlockListByStorageLocation(),
          fullStoreMeta.getLostStorage(), configList);
    } else {
      mMasterClient.register(workerId,
          fullStoreMeta.getStorageTierAssoc().getOrderedStorageAliases(),
          fullStoreMeta.getCapacityBytesOnTiers(),
          fullStoreMeta.getUsedBytesOnTiers(), fullStoreMeta.getBlockListByStorageLocation(),
          fullStoreMeta.getLostStorage(), configList);
    }
  }

  /**
   * heartbeats to the master and handles master heartbeat command.
   * Errors are handled in the method.
   * @param workerId the worker id
   * @param blockReport the block report
   * @param storeMeta the store meta
   * @param handler the command handler
   * @return true if the heartbeat succeeded
   */
  boolean heartbeat(
      long workerId, BlockHeartbeatReport blockReport, BlockStoreMeta storeMeta,
      MasterCommandHandler handler
  ) {
    // Send the heartbeat and execute the response
    Command cmdFromMaster = null;
    List<alluxio.grpc.Metric> metrics = MetricsSystem.reportWorkerMetrics();

    try {
      cmdFromMaster = mMasterClient.heartbeat(workerId, storeMeta.getCapacityBytesOnTiers(),
          storeMeta.getUsedBytesOnTiers(), blockReport.getRemovedBlocks(),
          blockReport.getAddedBlocks(), blockReport.getLostStorage(), metrics);
      handler.handle(cmdFromMaster);
      return true;
    } catch (Exception e) {
      // An error occurred, log and ignore it or error if heartbeat timeout is reached
      if (cmdFromMaster == null) {
        LOG.error("Failed to receive master heartbeat command. worker id {}", workerId, e);
      } else {
        LOG.error("Failed to receive or execute master heartbeat command: {}. worker id {}",
            cmdFromMaster, workerId, e);
      }
      mMasterClient.disconnect();
      return false;
    }
  }
}
