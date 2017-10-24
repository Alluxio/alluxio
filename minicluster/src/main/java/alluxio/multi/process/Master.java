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

package alluxio.multi.process;

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
  private final MasterNetAddress mAddress;

  private ExternalProcess mProcess;

  /**
   * @param confDir configuration directory
   * @param logsDir logs directory
   * @param address address information for the master
   */
  public Master(File confDir, File logsDir, MasterNetAddress address) throws IOException {
    mConfDir = confDir;
    mLogsDir = logsDir;
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
    mProcess =
        new ExternalProcess(conf, LimitedLifeMasterProcess.class, new File(mLogsDir, "master.out"));
    mProcess.start();
  }

  @Override
  public synchronized void close() {
    if (mProcess != null) {
      mProcess.stop();
      mProcess = null;
    }
  }
}
