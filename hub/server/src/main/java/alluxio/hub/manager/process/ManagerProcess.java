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

package alluxio.hub.manager.process;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.ServerConfiguration;
import alluxio.hub.common.ConfigOverrides;
import alluxio.hub.manager.rpc.ManagerRpcServer;
import alluxio.util.CommonUtils;
import alluxio.util.LogUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The main class responsible for launching captains or pirates.
 */
public class ManagerProcess implements AutoCloseable {
  public static final Logger LOG = LoggerFactory.getLogger(ManagerProcess.class);

  private final ManagerRpcServer mRpcServer;
  private final ManagerProcessContext mContext;

  /**
   * Creates a new {@link ManagerProcess}.
   * @param conf the alluxio configuration
   * @throws IOException if any of the servers fail to start
   */
  public ManagerProcess(AlluxioConfiguration conf) throws IOException {
    CommonUtils.PROCESS_TYPE.set(CommonUtils.ProcessType.HUB_MANAGER);
    mContext = new ManagerProcessContext(conf);
    mRpcServer = new ManagerRpcServer(conf, mContext);
  }

  /**
   * Blocks until the RPC server is terminated.
   */
  public void awaitTermination() {
    mRpcServer.awaitTermination();
  }

  @Override
  public void close() throws Exception {
    mRpcServer.close();
    mContext.close();
  }

  /**
   * @return the running RPC server port
   */
  public int getRpcPort() {
    return mRpcServer.getPort();
  }

  /**
   * @return whether the RPC server is serving requests
   */
  public boolean isServing() {
    return mRpcServer.isServing();
  }

  /**
   * Launch the captain process.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    LOG.info("The hub manager is starting!");
    ConfigOverrides.overrideConfigs(ServerConfiguration.global());
    try (ManagerProcess process = new ManagerProcess(ServerConfiguration.global())) {
      process.awaitTermination();
    } catch (Exception e) {
      LogUtils.warnWithException(LOG, "hub manager server not terminated properly.", e);
      System.exit(1);
    }
    LOG.info("The hub manager is shut down now.");
    System.exit(0);
  }
}
