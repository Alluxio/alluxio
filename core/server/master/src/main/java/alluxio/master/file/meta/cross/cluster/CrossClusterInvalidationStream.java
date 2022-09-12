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

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.PathInvalidation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Stream for cross cluster invalidations, received at publisher.
 */
public class CrossClusterInvalidationStream {
  private static final Logger LOG = LoggerFactory.getLogger(CrossClusterInvalidationStream.class);

  private final ServerCallStreamObserver<PathInvalidation> mInvalidationStream;
  private boolean mCompleted = false;
  private final MountSync mMountSync;
  private final long mReadyMaxWait = Configuration.getMs(
      PropertyKey.MASTER_CROSS_CLUSTER_INVALIDATION_QUEUE_WAIT);

  /**
   * @param mountSync the mount information
   * @param invalidationStream the invalidation stream
   */
  public CrossClusterInvalidationStream(
      MountSync mountSync, StreamObserver<PathInvalidation> invalidationStream) {
    mMountSync = mountSync;
    mInvalidationStream = (ServerCallStreamObserver<PathInvalidation>) invalidationStream;
    mInvalidationStream.setOnReadyHandler(() -> {
      synchronized (this) {
        notifyAll();
      }
    });
  }

  /**
   * @return the invalidation stream observer
   */
  @VisibleForTesting
  public ServerCallStreamObserver<PathInvalidation> getInvalidationStreamObserver() {
    return mInvalidationStream;
  }

  /**
   * @return the mount info of the stream
   */
  public MountSync getMountSync() {
    return mMountSync;
  }

  /**
   * @return the completed state of the stream
   */
  public synchronized boolean getCompleted() {
    return mCompleted;
  }

  /**
   * @param ufsPath publish a path
   * @return true if the stream is not completed, false otherwise
   */
  public synchronized boolean publishPath(String ufsPath) {
    if (mCompleted) {
      return false;
    }
    try {
      if (mReadyMaxWait > 0 && !mInvalidationStream.isReady()) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        long remain = mReadyMaxWait;
        while (!mInvalidationStream.isReady() && remain > 0) {
          wait(remain);
          remain = mReadyMaxWait - stopwatch.elapsed(TimeUnit.MILLISECONDS);
        }
      }
      Preconditions.checkState(mInvalidationStream.isReady(),
          "Client %s not ready to receive cross cluster invalidation, closing connection",
          mMountSync);
      mInvalidationStream.onNext(PathInvalidation.newBuilder().setUfsPath(ufsPath).build());
    } catch (Exception e) {
      LOG.warn("Error while trying to publish invalidation path", e);
      onError(e);
      return false;
    }
    return true;
  }

  /**
   * Called on completion.
   */
  public synchronized void onCompleted() {
    if (!mCompleted) {
      try {
        mInvalidationStream.onCompleted();
      } catch (Exception e) {
        LOG.warn("Error completing the invalidation stream", e);
      }
      mCompleted = true;
    }
  }

  /**
   * Called on error.
   * @param t the error
   */
  public synchronized void onError(Throwable t) {
    if (!mCompleted) {
      try {
        mInvalidationStream.onError(t);
      } catch (Exception e) {
        e.addSuppressed(t);
        LOG.warn("Error closing the invalidation stream from an error", e);
      }
      mCompleted = true;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof CrossClusterInvalidationStream) {
      return ((CrossClusterInvalidationStream) o).mMountSync.equals(mMountSync);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return mMountSync.hashCode();
  }
}
