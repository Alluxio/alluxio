/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.client.file.TachyonFileSystem;
import tachyon.conf.TachyonConf;
import tachyon.exception.ConnectionFailedException;
import tachyon.util.io.PathUtils;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.worker.block.BlockWorker;
import tachyon.worker.lineage.LineageWorker;

public abstract class AbstractLocalTachyonCluster {
  protected static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  protected long mWorkerCapacityBytes;
  protected int mUserBlockSize;
  protected int mQuotaUnitBytes = 1000;

  protected TachyonConf mMasterConf;
  protected TachyonConf mWorkerConf;

  protected BlockWorker mWorker;
  protected LineageWorker mLineageWorker;

  protected String mTachyonHome;
  protected String mHostname;

  protected Thread mWorkerThread;

  public AbstractLocalTachyonCluster(long workerCapacityBytes, int userBlockSize) {
    mWorkerCapacityBytes = workerCapacityBytes;
    mUserBlockSize = userBlockSize;
  }

  public abstract TachyonFileSystem getClient() throws IOException;

  public abstract int getMasterPort();

  public TachyonConf getMasterTachyonConf() {
    return mMasterConf;
  }

  public abstract void start() throws IOException, ConnectionFailedException;

  protected void resetContext() {}

  protected void setHostname() {
    mHostname = NetworkAddressUtils.getLocalHostName(100);
  }

  protected void setTachyonHome() throws IOException {
    mTachyonHome =
        File.createTempFile("Tachyon", "U" + System.currentTimeMillis()).getAbsolutePath();
  }

  public void stop() throws Exception {
    stopTFS();
    stopUFS();

    resetContext();
  }

  public abstract void stopTFS() throws Exception;

  public abstract void stopUFS() throws Exception;

  public TachyonConf newTestConf() throws IOException {
    TachyonConf testConf = new TachyonConf();
    setTachyonHome();
    setHostname();

    testConf.set(Constants.IN_TEST_MODE, "true");
    testConf.set(Constants.TACHYON_HOME, mTachyonHome);
    testConf.set(Constants.USER_QUOTA_UNIT_BYTES, Integer.toString(mQuotaUnitBytes));
    testConf.set(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT, Integer.toString(mUserBlockSize));
    testConf.set(Constants.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES, Integer.toString(64));
    testConf.set(Constants.MASTER_HOSTNAME, mHostname);
    testConf.set(Constants.MASTER_PORT, Integer.toString(0));
    testConf.set(Constants.MASTER_WEB_PORT, Integer.toString(0));
    testConf.set(Constants.MASTER_TTLCHECKER_INTERVAL_MS, Integer.toString(1000));
    testConf.set(Constants.MASTER_WORKER_THREADS_MIN, "1");
    testConf.set(Constants.MASTER_WORKER_THREADS_MAX, "100");
    testConf.set(Constants.THRIFT_STOP_TIMEOUT_SECONDS, "0");

    // If tests fail to connect they should fail early rather than using the default ridiculously
    // high retries
    testConf.set(Constants.MASTER_RETRY_COUNT, "3");

    // Since tests are always running on a single host keep the resolution timeout low as otherwise
    // people running with strange network configurations will see very slow tests
    testConf.set(Constants.NETWORK_HOST_RESOLUTION_TIMEOUT_MS, "250");

    testConf.set(Constants.WEB_THREAD_COUNT, "1");
    testConf.set(Constants.WEB_RESOURCES,
            PathUtils.concatPath(System.getProperty("user.dir"), "../servers/src/main/webapp"));

    // default write type becomes MUST_CACHE, set this value to CACHE_THROUGH for tests.
    // default tachyon storage is STORE, and under storage is SYNC_PERSIST for tests.
    // TODO(binfan): eliminate this setting after updating integration tests
    testConf.set(Constants.USER_FILE_WRITE_TYPE_DEFAULT, "CACHE_THROUGH");

    testConf.set(Constants.WORKER_PORT, Integer.toString(0));
    testConf.set(Constants.WORKER_DATA_PORT, Integer.toString(0));
    testConf.set(Constants.WORKER_WEB_PORT, Integer.toString(0));
    testConf.set(Constants.WORKER_DATA_FOLDER, "/datastore");
    testConf.set(Constants.WORKER_MEMORY_SIZE, Long.toString(mWorkerCapacityBytes));
    testConf.set(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS, Integer.toString(15));
    testConf.set(Constants.WORKER_WORKER_BLOCK_THREADS_MIN, Integer.toString(1));
    testConf.set(Constants.WORKER_WORKER_BLOCK_THREADS_MAX, Integer.toString(2048));
    testConf.set(Constants.WORKER_NETWORK_NETTY_WORKER_THREADS, Integer.toString(2));

    // Perform immediate shutdown of data server. Graceful shutdown is unnecessary and slow
    testConf.set(Constants.WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD, Integer.toString(0));
    testConf.set(Constants.WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT, Integer.toString(0));

    // Setup tiered store
    String ramdiskPath = PathUtils.concatPath(mTachyonHome, "ramdisk");
    testConf.set(String.format(Constants.WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT, 0), "MEM");
    testConf.set(String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT, 0),
            ramdiskPath);
    testConf.set(String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA_FORMAT, 0),
            Long.toString(mWorkerCapacityBytes));

    int numLevel = testConf.getInt(Constants.WORKER_TIERED_STORE_LEVELS);
    for (int level = 1; level < numLevel; level ++) {
      String tierLevelDirPath =
              String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT, level);
      String[] dirPaths = testConf.get(tierLevelDirPath).split(",");
      List<String> newPaths = new ArrayList<String>();
      for (String dirPath : dirPaths) {
        String newPath = mTachyonHome + dirPath;
        newPaths.add(newPath);
      }
      testConf.set(String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT, level),
              Joiner.on(',').join(newPaths));
    }
    return testConf;
  }
}
