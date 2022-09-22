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

package alluxio.master.file.meta.cross.cluster;

import alluxio.client.cross.cluster.CrossClusterClient;
import alluxio.client.cross.cluster.CrossClusterClientContextBuilder;
import alluxio.client.cross.cluster.RetryHandlingCrossClusterMasterClient;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.file.meta.InvalidationSyncCache;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.util.ConfigurationUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;

/**
 * Keeps the state of the cross cluster objects on the master.
 */
public class CrossClusterMasterState implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(CrossClusterMasterState.class);

  final boolean mCrossClusterEnabled =
      Configuration.getBoolean(PropertyKey.MASTER_CROSS_CLUSTER_ENABLE);

  /** Connection to the cross cluster configuration service. */
  CrossClusterClient mRunnerClient = mCrossClusterEnabled
      ? new RetryHandlingCrossClusterMasterClient(CrossClusterClientContextBuilder.create().build())
      : null;
  /** Connection to update mounts with the configuration service. */
  CrossClusterClient mUpdateMountClient = mCrossClusterEnabled
      ? new RetryHandlingCrossClusterMasterClient(CrossClusterClientContextBuilder.create().build())
      : null;

  final String mClusterId = mCrossClusterEnabled
      ? Configuration.getString(PropertyKey.MASTER_CROSS_CLUSTER_ID)
      : "CrossClusterDisabledID";

  /** Used to publish modifications to paths using cross cluster sync. */
  final CrossClusterPublisher mCrossClusterPublisher = mCrossClusterEnabled
      ? new DirectCrossClusterPublisher() : new NoOpCrossClusterPublisher();

  /** Used to update the configuration service with mount changes. */
  final CrossClusterMountClientRunner mCrossClusterMountClientRunner = mCrossClusterEnabled
      ? new CrossClusterMountClientRunner(mRunnerClient, mUpdateMountClient) : null;

  /** Used to maintain a streaming connection to the configuration service
   * to update the local state when an external cluster's mount changes. */
  final CrossClusterMountSubscriber mCrossClusterMountSubscriber;

  /** Tracks changes to local mounts. */
  final LocalMountState mLocalMountState;
  /**
   * Tracks changes to all cross cluster mounts, and creating subscriptions
   * to external clusters.
   */
  private final CrossClusterMount mCrossClusterMount;

  /**
   * Object storing state for cross cluster synchronization on the file system master.
   * @param syncCache the invalidation sync cache
   * @param mountTable the mount table
   */
  public CrossClusterMasterState(InvalidationSyncCache syncCache, MountTable mountTable) {
    mCrossClusterMount = new CrossClusterMount(mClusterId, syncCache);
    mLocalMountState = mCrossClusterEnabled ? new LocalMountState(mClusterId,
        ConfigurationUtils.getMasterRpcAddresses(Configuration.global())
            .toArray(new InetSocketAddress[0]),
        mCrossClusterMountClientRunner::onLocalMountChange,
        mCrossClusterMountClientRunner::beforeLocalMountChange) : null;
    mCrossClusterMountSubscriber = mCrossClusterEnabled ? new CrossClusterMountSubscriber(
        mClusterId, mRunnerClient, mCrossClusterMount,
        () -> {
          // this function is called on reconnection to the cross cluster
          // configuration service, at this point, we must send our
          // current mount list to the service, and invalidate
          // our local ufs sync caches for cross cluster mounts
          // in case there were modifications while we were disconnected
          mCrossClusterMountClientRunner.onReconnection();
          mountTable.invalidateCrossClusterMountCaches();
        }) : null;
    if (mCrossClusterEnabled) {
      mCrossClusterMountSubscriber.run();
      mCrossClusterMountClientRunner.run();
    }
  }

  /**
   * Starts the cross cluster services on the master.
   */
  public void start() {
    if (mCrossClusterEnabled) {
      mCrossClusterMountClientRunner.start();
      mCrossClusterMountSubscriber.start();
    }
  }

  /**
   * Stops the cross cluster services on the master.
   */
  public void stop() {
    if (mCrossClusterEnabled) {
      mCrossClusterMountClientRunner.stop();
      mCrossClusterMountSubscriber.stop();
    }
  }

  /**
   * @return the cross cluster publisher
   */
  public CrossClusterPublisher getCrossClusterPublisher() {
    return mCrossClusterPublisher;
  }

  /**
   * @return the local mount state object responsible for tracking changes to local mounts
   */
  public Optional<LocalMountState> getLocalMountState() {
    return Optional.ofNullable(mLocalMountState);
  }

  /**
   * @return the cross cluster mount object responsible to keeping subscriptions to other clusters
   */
  public Optional<CrossClusterMount> getCrossClusterMount() {
    return Optional.ofNullable(mCrossClusterMount);
  }

  /**
   * This should be called before adding a local mount.
   * It will throw an exception if unable to update the
   * cross cluster naming service with the new mount info.
   * @param mountInfo the mount info
   */
  public void beforeAddLocalMount(MountInfo mountInfo) {
    getLocalMountState().ifPresent(mountState
        -> mountState.beforeAddMount(mountInfo));
  }

  /**
   * Adds local mount information.
   * @param mountInfo the added mount
   */
  public void addLocalMount(MountInfo mountInfo) {
    getCrossClusterMount().ifPresent(mountState ->
        mountState.addLocalMount(mountInfo));
    getLocalMountState().ifPresent(mountState ->
        mountState.addMount(mountInfo));
  }

  /**
   * Removes local mount information.
   * @param removed the removed mount
   */
  public void removeLocalMount(MountInfo removed) {
    getCrossClusterMount().ifPresent(mountState ->
        mountState.removeLocalMount(removed));
    getLocalMountState().ifPresent(mountState ->
        mountState.removeMount(removed));
  }

  /**
   * Reset state about local mount information.
   */
  public void resetState() {
    if (mCrossClusterEnabled) {
      mCrossClusterMount.resetState();
      mLocalMountState.resetState();
    }
  }

  @Override
  public void close() throws IOException {
    if (mCrossClusterEnabled) {
      mCrossClusterMount.close();
      mCrossClusterMountClientRunner.close();
      mCrossClusterMountSubscriber.close();
      mRunnerClient.close();
      mCrossClusterPublisher.close();
    }
  }

  /**
   * Update the address of the cross cluster configuration service.
   * @param addresses the new addresses
   */
  public synchronized void updateCrossClusterConfigurationAddress(InetSocketAddress[] addresses) {
    if (mCrossClusterEnabled) {
      StringBuilder builder = new StringBuilder(addresses.length * 3);
      for (int i = 0; i < addresses.length; i++) {
        builder.append(addresses[i].getHostString());
        builder.append(":");
        builder.append(addresses[i].getPort());
        if (i != addresses.length - 1) {
          builder.append(";");
        }
      }
      CrossClusterClient prevClient = mRunnerClient;
      Configuration.set(PropertyKey.MASTER_CROSS_CLUSTER_RPC_ADDRESSES, builder.toString());
      mRunnerClient = new RetryHandlingCrossClusterMasterClient(
          CrossClusterClientContextBuilder.create().build());
      mUpdateMountClient = new RetryHandlingCrossClusterMasterClient(
          CrossClusterClientContextBuilder.create().build());
      mCrossClusterMountSubscriber.changeClient(mRunnerClient);
      mCrossClusterMountClientRunner.changeClient(mRunnerClient, mUpdateMountClient);
      try {
        prevClient.close();
      } catch (Exception e) {
        LOG.warn("Error closing cross cluster client", e);
      }
    }
  }
}
