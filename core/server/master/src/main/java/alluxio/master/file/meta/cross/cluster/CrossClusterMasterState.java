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

import alluxio.ClientContext;
import alluxio.client.cross.cluster.CrossClusterClient;
import alluxio.client.cross.cluster.CrossClusterClientContextBuilder;
import alluxio.client.cross.cluster.RetryHandlingCrossClusterMasterClient;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.util.ConfigurationUtils;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;

/**
 * Keeps the state of the cross cluster objects on the master.
 */
public class CrossClusterMasterState implements Closeable {

  final boolean mCrossClusterEnabled =
      Configuration.getBoolean(PropertyKey.MASTER_CROSS_CLUSTER_ENABLE);

  /** Connection to the cross cluster configuration service. */
  final CrossClusterClient mCrossClusterClient = mCrossClusterEnabled
      ? new RetryHandlingCrossClusterMasterClient(new CrossClusterClientContextBuilder(
      ClientContext.create()).build()) : null;

  /** Used to publish modifications to paths using cross cluster sync. */
  final CrossClusterPublisher mCrossClusterPublisher = mCrossClusterEnabled
      ? new DirectCrossClusterPublisher() : new NoOpCrossClusterPublisher();

  /** Used to update the configuration service with mount changes. */
  final CrossClusterMountClientRunner mCrossClusterMountClientRunner = mCrossClusterEnabled
      ? new CrossClusterMountClientRunner(mCrossClusterClient) : null;

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
   * @param clusterId the local cluster id
   * @param syncCache the invalidation sync cache
   */
  public CrossClusterMasterState(String clusterId, InvalidationSyncCache syncCache) {
    mCrossClusterMount = new CrossClusterMount(clusterId, syncCache,
        ignored -> { }, ignored -> { });
    mLocalMountState = mCrossClusterEnabled ? new LocalMountState(clusterId,
        ConfigurationUtils.getMasterRpcAddresses(Configuration.global())
            .toArray(new InetSocketAddress[0]),
        mCrossClusterMountClientRunner::onLocalMountChange) : null;
    mCrossClusterMountSubscriber = mCrossClusterEnabled ? new CrossClusterMountSubscriber(
        clusterId, mCrossClusterClient, mCrossClusterMount) : null;
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
   * @return the client for connecting to cross cluster configuration services
   */
  public Optional<CrossClusterClient> getCrossClusterClient() {
    return Optional.ofNullable(mCrossClusterClient);
  }

  /**
   * @return true if cross cluster synchronization is enabled
   */
  public boolean isCrossClusterEnabled() {
    return mCrossClusterEnabled;
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
   * Adds local mount information.
   * @param mountInfo the addded mount
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
      mCrossClusterClient.close();
      mCrossClusterPublisher.close();
    }
  }
}
