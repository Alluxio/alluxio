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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Verify;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Publisher for direct cluster to cluster subscriptions from other clusters.
 */
public class DirectCrossClusterPublisher implements CrossClusterPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(DirectCrossClusterPublisher.class);

  private final CrossClusterIntersection<CrossClusterInvalidationStream> mCrossClusterIntersection
      = new CrossClusterIntersection<>();
  private final ConcurrentHashMap<MountSync, CrossClusterInvalidationStream> mMountSyncToStream =
      new ConcurrentHashMap<>();

  /**
   * Create a direct cross cluster publisher.
   */
  public DirectCrossClusterPublisher() {}

  /**
   * @param mountSync the mount sync info
   * @return the stream for the given sync info (if it exists)
   */
  @VisibleForTesting
  public Optional<CrossClusterInvalidationStream> getStreamForMountSync(MountSync mountSync) {
    return Optional.ofNullable(mMountSyncToStream.get(mountSync));
  }

  /**
   * Add a subscriber.
   * @param stream the stream to place invalidations on
   */
  public void addSubscriber(CrossClusterInvalidationStream stream) {
    LOG.info("Received a cross cluster subscription from {}", stream.getMountSync());
    mMountSyncToStream.compute(stream.getMountSync(), (key, prevStream) -> {
      if (prevStream != null) {
        LOG.info("Removing previous cross cluster subscription from {}", stream.getMountSync());
      }
      mCrossClusterIntersection.addMapping(stream.getMountSync().getClusterId(),
          stream.getMountSync().getUfsPath(), stream);
      Verify.verify(stream.publishPath(stream.getMountSync().getUfsPath()),
          "Unable to publish invalidation on initial subscription %s",
          stream.getMountSync());
      return stream;
    });
  }

  @Override
  public void publish(String ufsPath) {
    mCrossClusterIntersection.getClusters(ufsPath).forEach((stream) -> {
      if (stream != null) {
        LOG.debug("Publishing invalidation of path {} to {}", ufsPath, stream.getMountSync());
        if (!stream.publishPath(ufsPath)) {
          LOG.info("Removing a cross cluster subscription from {}"
                  + " due to stream being not being active", stream.getMountSync());
          mCrossClusterIntersection.removeMapping(stream.getMountSync().getClusterId(),
              stream.getMountSync().getUfsPath(),
              CrossClusterInvalidationStream::getCompleted);
        }
      }
    });
  }

  @Override
  public void close() throws IOException {
    mMountSyncToStream.entrySet().removeIf((entry) -> {
      entry.getValue().onCompleted();
      return true;
    });
  }
}
