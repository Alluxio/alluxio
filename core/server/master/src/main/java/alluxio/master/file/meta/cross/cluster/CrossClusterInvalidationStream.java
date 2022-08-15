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

import alluxio.grpc.PathInvalidation;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stream for cross cluster invalidations, received at publisher.
 */
public class CrossClusterInvalidationStream {
  private static final Logger LOG = LoggerFactory.getLogger(CrossClusterInvalidationStream.class);

  private final StreamObserver<PathInvalidation> mInvalidationStream;
  private boolean mCompleted = false;
  private final MountSync mMountSync;

  /**
   * @param mountSync the mount information
   * @param invalidationStream the invalidation stream
   */
  public CrossClusterInvalidationStream(
      MountSync mountSync, StreamObserver<PathInvalidation> invalidationStream) {
    mInvalidationStream = invalidationStream;
    mMountSync = mountSync;
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
  public boolean getCompleted() {
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
      mInvalidationStream.onNext(PathInvalidation.newBuilder().setUfsPath(ufsPath).build());
    } catch (Exception e) {
      LOG.warn("Error while trying to publish invalidation path", e);
      mCompleted = true;
      return false;
    }
    return true;
  }

  /**
   * Called on completion.
   */
  public synchronized void onCompleted() {
    if (!mCompleted) {
      mInvalidationStream.onCompleted();
      mCompleted = true;
    }
  }

  /**
   * Called on error.
   * @param t the error
   */
  public synchronized void onError(Throwable t) {
    if (!mCompleted) {
      mInvalidationStream.onError(t);
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
