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

package alluxio;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility methods for the configuration tests.
 */
public final class ConfigurationTestUtils {

  /**
   * Return an instanced configuration with default value from the site properties file.
   * @return the default configuration
   */
  public static InstancedConfiguration defaults() {
    return new InstancedConfiguration(ConfigurationUtils.defaults());
  }

  /**
   * Returns reasonable default configuration values for running integration tests.
   *
   * These defaults are mostly aimed at getting tests to run faster. Individual tests may override
   * them to test specific functionality.
   *
   * @param hostname the master hostname
   * @param workDirectory the work directory in which to configure the journal and tiered storage
   * @return the configuration
   */
  public static Map<PropertyKey, String> testConfigurationDefaults(AlluxioConfiguration alluxioConf,
      String hostname, String workDirectory) {
    Map<PropertyKey, String> conf = new HashMap<>();
    conf.put(PropertyKey.MASTER_HOSTNAME, hostname);
    conf.put(PropertyKey.WORKER_BIND_HOST, hostname);
    conf.put(PropertyKey.WORKER_WEB_BIND_HOST, hostname);
    conf.put(PropertyKey.MASTER_BIND_HOST, hostname);
    conf.put(PropertyKey.MASTER_WEB_BIND_HOST, hostname);

    conf.put(PropertyKey.WORK_DIR, workDirectory);
    // Sets up the tiered store
    conf.put(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS.format(0), "MEM");
    String ramdiskPath = PathUtils.concatPath(workDirectory, "ramdisk");
    conf.put(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(0), ramdiskPath);

    int numLevel = alluxioConf.getInt(PropertyKey.WORKER_TIERED_STORE_LEVELS);
    for (int level = 1; level < numLevel; level++) {
      PropertyKey tierLevelDirPath =
          PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(level);
      String[] dirPaths = alluxioConf.get(tierLevelDirPath).split(",");
      List<String> newPaths = new ArrayList<>();
      for (String dirPath : dirPaths) {
        String newPath = workDirectory + dirPath;
        newPaths.add(newPath);
      }
      conf.put(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(level),
          Joiner.on(',').join(newPaths));
    }
    // Sets up the journal folder
    conf.put(PropertyKey.MASTER_JOURNAL_TYPE, "UFS");
    conf.put(PropertyKey.MASTER_JOURNAL_FOLDER, PathUtils.concatPath(workDirectory, "journal"));
    conf.put(PropertyKey.MASTER_METASTORE_DIR, PathUtils.concatPath(workDirectory, "metastore"));

    conf.put(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "1KB");
    conf.put(PropertyKey.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES, "64");
    conf.put(PropertyKey.USER_STREAMING_READER_CHUNK_SIZE_BYTES, "64");
    conf.put(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS, "1sec");
    conf.put(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "1sec");
    // This cannot be too short, since sometimes there are grpc channel startup delays, which
    // affect authentication
    conf.put(PropertyKey.NETWORK_CONNECTION_AUTH_TIMEOUT, "5sec");
    conf.put(PropertyKey.NETWORK_CONNECTION_SHUTDOWN_GRACEFUL_TIMEOUT, "3sec");
    conf.put(PropertyKey.NETWORK_CONNECTION_SERVER_SHUTDOWN_TIMEOUT, "10sec");

    // Shutdown journal tailer quickly. Graceful shutdown is unnecessarily slow.
    conf.put(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS, "50ms");
    conf.put(PropertyKey.MASTER_JOURNAL_TAILER_SLEEP_TIME_MS, "10ms");

    // To keep tests fast, we should do more retries with a lower max wait time.
    conf.put(PropertyKey.USER_RPC_RETRY_MAX_DURATION, "2s");
    conf.put(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS, "200");
    conf.put(PropertyKey.USER_RPC_RETRY_BASE_SLEEP_MS, "20");

    // Do not engage safe mode by default since the worker is connected when test starts.
    conf.put(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME, "0sec");

    // Since tests are always running on a single host keep the resolution timeout low as otherwise
    // people running with strange network configurations will see very slow tests
    conf.put(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS, "250ms");

    // default write type becomes MUST_CACHE, set this value to CACHE_THROUGH for tests.
    // default Alluxio storage is STORE, and under storage is SYNC_PERSIST for tests.
    // TODO(binfan): eliminate this setting after updating integration tests
    //conf.put(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, "CACHE_THROUGH");

    conf.put(PropertyKey.WEB_THREADS, "1");
    conf.put(PropertyKey.WEB_RESOURCES,
        PathUtils.concatPath(System.getProperty("user.dir"), "../webui"));
    conf.put(PropertyKey.WORKER_RAMDISK_SIZE, "100MB");
    conf.put(PropertyKey.MASTER_LOST_WORKER_FILE_DETECTION_INTERVAL, "15ms");
    conf.put(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS, "15ms");
    conf.put(PropertyKey.WORKER_NETWORK_NETTY_WORKER_THREADS, "2");

    // Shutdown data server quickly. Graceful shutdown is unnecessarily slow.
    conf.put(PropertyKey.WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD, "0ms");
    conf.put(PropertyKey.WORKER_NETWORK_SHUTDOWN_TIMEOUT, "0ms");

    conf.put(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS.format(0), "MEM");

    conf.put(PropertyKey.MASTER_EMBEDDED_JOURNAL_ELECTION_TIMEOUT, "260ms");
    conf.put(PropertyKey.MASTER_EMBEDDED_JOURNAL_HEARTBEAT_INTERVAL, "50ms");
    // Reset the value to avoid raft journal system complaining about log size < 65
    conf.put(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX,
        PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX.getDefaultValue());
    // For faster test shutdown
    conf.put(PropertyKey.MASTER_EMBEDDED_JOURNAL_SHUTDOWN_TIMEOUT, "100ms");
    conf.put(PropertyKey.USER_WORKER_LIST_REFRESH_INTERVAL, "1s");

    // faster persists
    conf.put(PropertyKey.JOB_MASTER_WORKER_HEARTBEAT_INTERVAL, "20ms");
    conf.put(PropertyKey.MASTER_PERSISTENCE_CHECKER_INTERVAL_MS, "20ms");
    conf.put(PropertyKey.MASTER_PERSISTENCE_INITIAL_INTERVAL_MS, "20ms");
    conf.put(PropertyKey.MASTER_PERSISTENCE_SCHEDULER_INTERVAL_MS, "20ms");

    // faster refresh
    conf.put(PropertyKey.MASTER_WORKER_INFO_CACHE_REFRESH_TIME, "20ms");

    // faster I/O retries.
    conf.put(PropertyKey.USER_BLOCK_READ_RETRY_SLEEP_MIN, "1ms");
    conf.put(PropertyKey.USER_BLOCK_READ_RETRY_SLEEP_MIN, "5ms");
    conf.put(PropertyKey.USER_BLOCK_READ_RETRY_MAX_DURATION, "10ms");

    return conf;
  }

  private ConfigurationTestUtils() {} // prevent instantiation
}
