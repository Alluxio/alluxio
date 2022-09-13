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

import com.google.common.annotations.VisibleForTesting;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

/**
 * A testing implementation of {@link CrossClusterPublisher} that tracks paths published
 * in a list.
 */
@VisibleForTesting
public class TrackingCrossClusterPublisher {

  static class TrackingStream extends ServerCallStreamObserver<PathInvalidation> {

    Collection<String> mPaths = new ConcurrentLinkedQueue<>();
    private boolean mCompleted = false;

    @Override
    public void onNext(PathInvalidation value) {
      if (mCompleted) {
        throw new RuntimeException("called onNext after completed");
      }
      mPaths.add(value.getUfsPath());
    }

    @Override
    public void onError(Throwable t) {
      throw new RuntimeException(t);
    }

    @Override
    public void onCompleted() {
      mCompleted = true;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public void setOnCancelHandler(Runnable onCancelHandler) {
    }

    @Override
    public void setCompression(String compression) {
    }

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public void setOnReadyHandler(Runnable onReadyHandler) {
    }

    @Override
    public void disableAutoInboundFlowControl() {
    }

    @Override
    public void request(int count) {
    }

    @Override
    public void setMessageCompression(boolean enable) {
    }
  }

  ConcurrentHashMap<MountSync, TrackingStream> mPublishedPaths =
      new ConcurrentHashMap<>();

  /**
   * Tracks the published paths by cluster name for testing.
   */
  public TrackingCrossClusterPublisher() {
  }

  /**
   * Get the tracking stream for the given mount info.
   * @param mountSync the mount info
   * @return the tracking stream
   */
  public StreamObserver<PathInvalidation> getStream(MountSync mountSync) {
    return mPublishedPaths.computeIfAbsent(mountSync, (key) -> new TrackingStream());
  }

  /**
   * @param mountSync the cluster info
   * @return the published paths for the cluster id
   */
  public Stream<String> getPublishedPaths(MountSync mountSync) {
    return mPublishedPaths.getOrDefault(mountSync, new TrackingStream()).mPaths.stream();
  }
}
