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

package alluxio.external;

import alluxio.PropertyKey;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for running and interacting with an Alluxio master in a separate process.
 */
@ThreadSafe
public final class Master implements Closeable {
  private final File mLogsDir;
  private final File mConfDir;
  private final File mOutFile;
  private final MasterNetAddress mAddress;

  private ExternalProcess mProcess;

  /**
   * @param mWorkDir the work directory to use for the master process
   * @param address the address information for the master
   * @param masterId an ID for this master, used to distinguish it from other masters in the same
   *        cluster
   */
  public Master(File mWorkDir, int masterId, MasterNetAddress address) throws IOException {
    mLogsDir = new File(mWorkDir, "logs-master" + masterId);
    mConfDir = new File(mWorkDir, "conf");
    mOutFile = new File(mLogsDir, "master.out");
    mAddress = address;
  }

  /**
   * Launches the master process.
   */
  public synchronized void start() throws IOException {
    mLogsDir.mkdirs();
    Map<PropertyKey, Object> conf = new HashMap<>();
    conf.put(PropertyKey.LOGGER_TYPE, "MASTER_LOGGER");
    conf.put(PropertyKey.CONF_DIR, mConfDir.getAbsolutePath());
    conf.put(PropertyKey.LOGS_DIR, mLogsDir.getAbsolutePath());
    conf.put(PropertyKey.MASTER_HOSTNAME, mAddress.getHostname());
    conf.put(PropertyKey.MASTER_RPC_PORT, mAddress.getRpcPort());
    conf.put(PropertyKey.MASTER_WEB_PORT, mAddress.getWebPort());
    mProcess = new ExternalProcess(conf, LimitedLifeMasterProcess.class, mOutFile);
    mProcess.start();
  }

  @Override
  public synchronized void close() {
    if (mProcess != null) {
      mProcess.close();
      mProcess = null;
    }
  }
}
