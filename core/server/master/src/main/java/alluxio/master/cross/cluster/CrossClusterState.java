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

package alluxio.master.cross.cluster;

import alluxio.grpc.MountList;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks the cross cluster state at the configuration process.
 */
public class CrossClusterState implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(CrossClusterState.class);

  private final ConcurrentHashMap<String, MountList> mMounts =
      new ConcurrentHashMap<>();
  private final Map<String, StreamObserver<MountList>> mStreams = new ConcurrentHashMap<>();

  /**
   * @return the map of cluster ids to mounts
   */
  @VisibleForTesting
  public ConcurrentHashMap<String, MountList> getMounts() {
    return mMounts;
  }

  /**
   * @return the map of cluster ids to streams
   */
  @VisibleForTesting
  public Map<String, StreamObserver<MountList>> getStreams() {
    return mStreams;
  }

  /**
   * Set the mount list for a cluster.
   *
   * @param mountList the mount list
   */
  public synchronized void setMountList(MountList mountList) {
    MountList prevMountList = mMounts.get(mountList.getClusterId());
    if (prevMountList != null) {
      if (prevMountList.equals(mountList)) {
        LOG.info("Received unchanged mount list {}", mountList);
        return;
      }
    }
    LOG.info("Received new mount list {}", mountList);
    mMounts.put(mountList.getClusterId(), mountList);
    mStreams.forEach((clusterId, stream) -> {
      if (!clusterId.equals(mountList.getClusterId())) {
        try {
          stream.onNext(mountList);
        } catch (Exception e) {
          LOG.warn("Error updating mount list on stream", e);
        }
      }
    });
  }

  /**
   * Set the stream for the given cluster id.
   *
   * @param clusterId the cluster id
   * @param stream    the stream
   */
  public synchronized void setStream(String clusterId, StreamObserver<MountList> stream) {
    LOG.info("Received stream for cluster {}", clusterId);
    mStreams.compute(clusterId, (key, oldStream) -> {
      if (oldStream != null) {
        try {
          oldStream.onCompleted();
        } catch (Exception e) {
          LOG.debug("Error completing old stream for cluster {}", clusterId, e);
        }
      }
      return stream;
    });
    mMounts.forEach((otherClusterId, mountList) -> {
      if (!clusterId.equals(otherClusterId)) {
        stream.onNext(mountList);
      }
    });
  }

  @Override
  public synchronized void close() throws IOException {
    mStreams.entrySet().removeIf((entry) -> {
      try {
        entry.getValue().onCompleted();
      } catch (Exception e) {
        LOG.debug("Error completing old stream during close", e);
      }
      return true;
    });
    mMounts.clear();
  }
}
