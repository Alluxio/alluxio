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
import alluxio.conf.Configuration;
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
public interface WorkerMetaStore {
  /**
   * @param key the key whose associated value is to be returned
   * @return Returns the value to which the specified key is mapped,
   * or null if this map contains no mapping for the key.
   */
  String get(String key);

  /**
   * Associates the specified value with the specified key in this map.
   *
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @throws IOException I/O error if persist fails
   */
  void set(String key, String value) throws IOException;

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
     * Creates an instance of {@link WorkerMetaStore}.
     * @param conf : the configuration to use
     * @return an instance of WorkerMetaStore
     * @throws RuntimeException RuntimeException if fails
     */
    public static WorkerMetaStore create(AlluxioConfiguration conf) {
      // Compatibility test, the test env does not have ${ALLUXIO_HOME} dir write permissions
      if (Configuration.getBoolean(PropertyKey.TEST_MODE)
          && !Configuration.isSetByUser(PropertyKey.WORKER_METASTORE_PATH)) {
        Path temTestPath;
        try {
          temTestPath = Files.createTempDirectory("test-" + UUID.randomUUID());
        } catch (IOException e) {
          throw new RuntimeException(
                  "WorkerMetaStore create test temporary a file failed", e);
        }
        Configuration.set(PropertyKey.WORKER_METASTORE_PATH, temTestPath.toString());
      }

      String defaultPath = PathUtils.concatPath(
          conf.get(PropertyKey.WORKER_METASTORE_PATH), CLUSTERID_FILE);
      try {
        // Try to create a DB file in the location specified by the configuration
        return new DefaultWorkerMetaStore(defaultPath);
      } catch (IOException e) {
        LOG.warn("WorkerMetaStore create failed");
        if (conf.getBoolean(PropertyKey.WORKER_MUST_PRESIST_CLUSTERID)) {
          throw new RuntimeException("WorkerMetaStore create a file "
              + defaultPath + " failed", e);
        } else {
          try {
            // Allows not to persist,
            // so use a temporary directory to ensure that the program can run normally
            LOG.info("Try to create a file in the system temporary directory");
            Path tmpPath = Files.createTempDirectory("tmp-" + UUID.randomUUID());
            return new DefaultWorkerMetaStore(PathUtils.concatPath(tmpPath, CLUSTERID_FILE));
          } catch (IOException ioException) {
            throw new RuntimeException(
                "WorkerMetaStore create temporary a file failed", e);
          }
        }
      }
    }
  }
}
