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

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.PathInvalidation;
import alluxio.grpc.PathSubscription;
import alluxio.resource.CloseableResource;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stream used for cross cluster path invalidation subscriptions.
 */
public class InvalidationStream implements ClientResponseObserver<PathSubscription,
    PathInvalidation> {
  private static final Logger LOG = LoggerFactory.getLogger(InvalidationStream.class);

  InvalidationSyncCache mInvalidationCache;
  MountSyncAddress mMountSync;
  CrossClusterMount mCrossClusterMount;
  ClientCallStreamObserver<PathSubscription> mRequestStream;
  private CloseableResource<FileSystemMasterClient> mClient;
  private boolean mCancelled = false;
  private final int mQueueSize =
      Configuration.getInt(PropertyKey.MASTER_CROSS_CLUSTER_INVALIDATION_QUEUE_SIZE);
  private final int mQueueRefresh =
      Configuration.getInt(PropertyKey.MASTER_CROSS_CLUSTER_INVALIDATION_QUEUE_REFRESH);
  private long mMsgRecvCount = 0;

  /**
   * Create a new invalidation stream.
   * @param mount the mount information where invalidations are coming from
   * @param invalidationCache the invalidation cache will be updated when invalidations are received
   *                          on the stream
   * @param crossClusterMount object tracking lists of clusters and their mounts
   */
  @VisibleForTesting
  public InvalidationStream(MountSyncAddress mount, InvalidationSyncCache invalidationCache,
                     CrossClusterMount crossClusterMount) {
    mInvalidationCache = invalidationCache;
    mMountSync = mount;
    mCrossClusterMount = crossClusterMount;
  }

  synchronized void setClient(CloseableResource<FileSystemMasterClient> client) {
    if (!mCancelled) {
      mClient = client;
    }
  }

  private synchronized void releaseClient() {
    if (mClient != null) {
      mClient.closeResource();
      mClient = null;
    }
  }

  /**
   * @return the stream observer
   */
  @VisibleForTesting
  public ClientCallStreamObserver<PathSubscription> getClientStream() {
    return mRequestStream;
  }

  /**
   * @return the mount info for the stream
   */
  public MountSyncAddress getMountSyncAddress() {
    return mMountSync;
  }

  /**
   * Cancel the invalidation stream.
   */
  public synchronized void cancel() {
    mCancelled = true;
    if (mRequestStream != null) {
      mRequestStream.cancel("Cancelled subscription stream to " + mMountSync, null);
    }
    releaseClient();
  }

  @Override
  public void onNext(PathInvalidation invalidation) {
    try {
      mMsgRecvCount++;
      if (mMsgRecvCount % mQueueRefresh == 0) {
        mRequestStream.request(mQueueRefresh);
      }
      mInvalidationCache.notifyUfsInvalidation(new AlluxioURI(invalidation.getUfsPath()));
    } catch (InvalidPathException e) {
      LOG.warn("Received invalid invalidation path", e);
    }
  }

  @Override
  public void onError(Throwable t) {
    LOG.warn("Error in path invalidation stream", t);
    mCrossClusterMount.removeStream(mMountSync, this);
    releaseClient();
  }

  @Override
  public void onCompleted() {
    mCrossClusterMount.removeStream(mMountSync, this);
    releaseClient();
  }

  @Override
  public synchronized void beforeStart(ClientCallStreamObserver<PathSubscription> requestStream) {
    if (mCancelled) {
      mRequestStream.cancel("Cancelled subscription stream to " + mMountSync, null);
    }
    mRequestStream = requestStream;
    mRequestStream.disableAutoRequestWithInitial(mQueueSize);
  }
}
