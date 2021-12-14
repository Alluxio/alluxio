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

import static alluxio.Constants.CLUSTERID_FILE;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.IdUtils;
import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

/**
 * Persistent info for worker.
 */
@NotThreadSafe
public interface BlockWorkerDB {
  /**
   * Return the clusterId saved in the persistent storage, if the cluster ID not found
   * return an {@link IdUtils#EMPTY_CLUSTER_ID} ID .
   *
   * @return a cluster id
   */
  String getClusterId();

  /**
   * Set the cluster id to the persistent storage.
   *
   * @param clusterId the cluster id to persist
   * @throws IOException I/O error if persist clusterId fails
   */
  void setClusterId(String clusterId) throws IOException;

  /**
   * Resets persistent storage, clear all information.
   * noting to do if persistence file dose not exist
   * @throws IOException I/O error if fails
   */
  void reset() throws IOException;

  /**
   * Factory for block worker DB .
   */
  class Factory {
    private static final Logger LOG = LoggerFactory.getLogger(BlockWorkerFactory.class);

    /**
     * Creates an instance of {@link BlockWorkerDB}.
     * @param conf : the configuration to use
     * @return an instance of BlockWorkerDB
     * @throws RuntimeException RuntimeException if fails
     */
    public static BlockWorkerDB create(AlluxioConfiguration conf) {
      try {
        // Try to create a DB file in the location specified by the configuration
        return new DefaultBlockWorkerDB(PathUtils.concatPath(
            conf.get(PropertyKey.WORKER_CLUSTERID_PATH), CLUSTERID_FILE));
      } catch (IOException e) {
        LOG.warn("DefaultBlockWorkerDB create failed");
        if (conf.getBoolean(PropertyKey.WORKER_MUST_PRESIST_CLUSTERID)) {
          throw new RuntimeException("DefaultBlockWorkerDB create failed: %s", e);
        } else {
          try {
            // Allows not to persist clusterId,
            // so use a temporary directory to ensure that the program can run normally
            LOG.info("Try to create a DB file in the system temporary directory");
            Path tmpPath = Files.createTempDirectory("tmp-" + UUID.randomUUID());
            return new DefaultBlockWorkerDB(PathUtils.concatPath(tmpPath, CLUSTERID_FILE));
          } catch (IOException ioException) {
            throw new RuntimeException(
                "DefaultBlockWorkerDB create failed in the system temporary directory: %s", e);
          }
        }
      }
    }
  }
}
