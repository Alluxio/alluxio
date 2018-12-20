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
   * Resets the configuration to its initial state.
   *
   * This method should only be used as a cleanup mechanism between tests. It should not be used
   * while any object may be using the {@link Configuration}.
   */
  public static void resetConfiguration() {
    Configuration.reset();
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
  public static Map<PropertyKey, String> testConfigurationDefaults(String hostname,
      String workDirectory) {
    Map<PropertyKey, String> conf = new HashMap<>();
    conf.put(PropertyKey.MASTER_HOSTNAME, hostname);
    conf.put(PropertyKey.WORKER_BIND_HOST, hostname);
    conf.put(PropertyKey.WORKER_DATA_BIND_HOST, hostname);
    conf.put(PropertyKey.WORKER_WEB_BIND_HOST, hostname);
    conf.put(PropertyKey.MASTER_BIND_HOST, hostname);
    conf.put(PropertyKey.MASTER_WEB_BIND_HOST, hostname);

    conf.put(PropertyKey.WORK_DIR, workDirectory);
    // Sets up the tiered store
    conf.put(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS.format(0), "MEM");
    String ramdiskPath = PathUtils.concatPath(workDirectory, "ramdisk");
    conf.put(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(0), ramdiskPath);

    int numLevel = Configuration.getInt(PropertyKey.WORKER_TIERED_STORE_LEVELS);
    for (int level = 1; level < numLevel; level++) {
      PropertyKey tierLevelDirPath =
          PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(level);
      String[] dirPaths = Configuration.get(tierLevelDirPath).split(",");
      List<String> newPaths = new ArrayList<>();
      for (String dirPath : dirPaths) {
        String newPath = workDirectory + dirPath;
        newPaths.add(newPath);
      }
      conf.put(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(level),
          Joiner.on(',').join(newPaths));
    }
    // Sets up the journal folder
    conf.put(PropertyKey.MASTER_JOURNAL_FOLDER, PathUtils.concatPath(workDirectory, "journal"));

    conf.put(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "1KB");
    conf.put(PropertyKey.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES, "64");
    conf.put(PropertyKey.USER_NETWORK_NETTY_READER_PACKET_SIZE_BYTES, "64");
    conf.put(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS, "1sec");
    conf.put(PropertyKey.MASTER_WORKER_THREADS_MIN, "1");
    conf.put(PropertyKey.MASTER_WORKER_THREADS_MAX, "100");
    conf.put(PropertyKey.MASTER_STARTUP_CONSISTENCY_CHECK_ENABLED, "false");
    conf.put(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "1sec");
    conf.put(PropertyKey.MASTER_THRIFT_SHUTDOWN_TIMEOUT, "0sec");
    conf.put(PropertyKey.MASTER_GRPC_CHANNEL_SHUTDOWN_TIMEOUT, "3sec");
    conf.put(PropertyKey.MASTER_GRPC_SERVER_SHUTDOWN_TIMEOUT, "3sec");

    // Shutdown journal tailer quickly. Graceful shutdown is unnecessarily slow.
    conf.put(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS, "50ms");
    conf.put(PropertyKey.MASTER_JOURNAL_TAILER_SLEEP_TIME_MS, "10ms");

    // To keep tests fast, we should do more retries with a lower max wait time.
    conf.put(PropertyKey.USER_RPC_RETRY_MAX_NUM_RETRY, "60");
    conf.put(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS, "500ms");

    // Do not engage safe mode by default since the worker is connected when test starts.
    conf.put(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME, "0sec");

    // Since tests are always running on a single host keep the resolution timeout low as otherwise
    // people running with strange network configurations will see very slow tests
    conf.put(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS, "250ms");

    // default write type becomes MUST_CACHE, set this value to CACHE_THROUGH for tests.
    // default Alluxio storage is STORE, and under storage is SYNC_PERSIST for tests.
    // TODO(binfan): eliminate this setting after updating integration tests
    conf.put(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, "CACHE_THROUGH");

    conf.put(PropertyKey.WEB_THREADS, "1");
    conf.put(PropertyKey.WEB_RESOURCES, PathUtils
        .concatPath(System.getProperty("user.dir"), "../core/server/common/src/main/webapp"));
    conf.put(PropertyKey.WORKER_MEMORY_SIZE, "100MB");
    conf.put(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS, "15ms");
    conf.put(PropertyKey.WORKER_BLOCK_THREADS_MIN, "1");
    conf.put(PropertyKey.WORKER_BLOCK_THREADS_MAX, "2048");
    conf.put(PropertyKey.WORKER_NETWORK_NETTY_WORKER_THREADS, "2");

    // Shutdown data server quickly. Graceful shutdown is unnecessarily slow.
    conf.put(PropertyKey.WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD, "0ms");
    conf.put(PropertyKey.WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT, "0ms");

    conf.put(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS.format(0), "MEM");
    conf.put(PropertyKey.USER_RPC_RETRY_MAX_DURATION, "1s");
    return conf;
  }

  private ConfigurationTestUtils() {} // prevent instantiation
}
