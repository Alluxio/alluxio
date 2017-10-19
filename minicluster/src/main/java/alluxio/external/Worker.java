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
import alluxio.network.PortUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for running and interacting with an Alluxio worker in a separate process.
 */
@ThreadSafe
public class Worker implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Worker.class);

  private final File mLogsDir;
  private final File mConfDir;
  private final File mOutFile;
  private final int mRpcPort;
  private final int mDataPort;
  private final int mWebPort;

  private ExternalProcess mProcess;

  /**
   * @param mBaseDir the base directory to use for the worker process
   * @param workerId an ID for this worker, used to distinguish it from other workers in the same
   *        cluster
   */
  public Worker(File mBaseDir, int workerId) throws IOException {
    mLogsDir = new File(mBaseDir, "logs-worker" + workerId);
    mConfDir = new File(mBaseDir, "conf");
    mOutFile = new File(mLogsDir, "worker.out");
    mRpcPort = PortUtils.getFreePort();
    mDataPort = PortUtils.getFreePort();
    mWebPort = PortUtils.getFreePort();
  }

  /**
   * Launches the worker process.
   */
  public synchronized void start() throws IOException {
    mLogsDir.mkdirs();
    Map<PropertyKey, Object> conf = new HashMap<>();
    conf.put(PropertyKey.LOGGER_TYPE, "WORKER_LOGGER");
    conf.put(PropertyKey.CONF_DIR, mConfDir.getAbsolutePath());
    conf.put(PropertyKey.LOGS_DIR, mLogsDir.getAbsolutePath());
    conf.put(PropertyKey.WORKER_RPC_PORT, mRpcPort);
    conf.put(PropertyKey.WORKER_DATA_PORT, mDataPort);
    conf.put(PropertyKey.WORKER_WEB_PORT, mWebPort);
    mProcess = new ExternalProcess(conf, LimitedLifeWorkerProcess.class, mOutFile);
    LOG.info("Starting worker with (rpc, data, web) ports ({}, {}, {})", mRpcPort, mDataPort,
        mWebPort);
    mProcess.start();
  }

  /**
   * @return the worker's rpc port
   */
  public int getRpcPort() {
    return mRpcPort;
  }

  /**
   * @return the worker's web port
   */
  public int getDataPort() {
    return mWebPort;
  }

  /**
   * @return the worker's web port
   */
  public int getWebPort() {
    return mWebPort;
  }

  @Override
  public synchronized void close() {
    if (mProcess != null) {
      mProcess.close();
      mProcess = null;
    }
  }
}
