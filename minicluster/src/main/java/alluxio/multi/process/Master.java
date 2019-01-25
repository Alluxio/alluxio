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

import alluxio.conf.PropertyKey;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for running and interacting with an Alluxio master in a separate process.
 */
@ThreadSafe
public final class Master implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Master.class);

  private final File mLogsDir;
  private final Map<PropertyKey, String> mProperties;

  private ExternalProcess mProcess;

  /**
   * @param logsDir logs directory
   * @param properties alluxio properties
   */
  public Master(File logsDir, Map<PropertyKey, String> properties) throws IOException {
    mLogsDir = logsDir;
    mProperties = properties;
  }

  /**
   * Updates the master's configuration. This update will take effect the next time the master
   * is started.
   *
   * @param key the conf key to update
   * @param value the value to set, or null to unset the key
   */
  public void updateConf(PropertyKey key, @Nullable String value) {
    if (value == null) {
      mProperties.remove(key);
    } else {
      mProperties.put(key, value);
    }
  }

  /**
   * Launches the master process.
   */
  public synchronized void start() {
    Preconditions.checkState(mProcess == null, "Master is already running");
    LOG.info("Starting master with port {}", mProperties.get(PropertyKey.MASTER_RPC_PORT));
    mProcess = new ExternalProcess(mProperties, LimitedLifeMasterProcess.class,
        new File(mLogsDir, "master.out"));
    try {
      mProcess.start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return the configuration used by this master
   */
  public Map<PropertyKey, String> getConf() {
    return ImmutableMap.copyOf(mProperties);
  }

  @Override
  public synchronized void close() {
    if (mProcess != null) {
      mProcess.stop();
      mProcess = null;
    }
  }
}
