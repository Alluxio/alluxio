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

package alluxio.hub.agent.process;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.ServerConfiguration;
import alluxio.hub.agent.rpc.AgentRpcServer;
import alluxio.hub.common.ConfigOverrides;
import alluxio.util.CommonUtils;
import alluxio.util.LogUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main class responsible for launching Hub agents.
 */
public class AgentProcess implements AutoCloseable {
  public static final Logger LOG = LoggerFactory.getLogger(AgentProcess.class);

  private final AgentProcessContext mContext;
  private final AgentRpcServer mRpcServer;

  /**
   * Creates a new {@link AgentProcess}.
   *
   * @param conf the alluxio configuration
   * @throws Exception if any of the servers fail to start
   */
  public AgentProcess(AlluxioConfiguration conf) throws Exception {
    CommonUtils.PROCESS_TYPE.set(CommonUtils.ProcessType.HUB_AGENT);
    mContext = new AgentProcessContext(conf);
    mRpcServer = new AgentRpcServer(conf, mContext);
  }

  /**
   * Blocks until the RPC server is terminated.
   */
  public void awaitTermination() {
    mRpcServer.awaitTermination();
  }

  @Override
  public void close() {
    mContext.close();
    mRpcServer.close();
  }

  /**
   * @return the running RPC server port
   */
  public int getRpcPort() {
    return mRpcServer.getPort();
  }

  /**
   * Launch the agent process.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    LOG.info("The hub agent is starting!");
    try {
      ConfigOverrides.overrideConfigs(ServerConfiguration.global());
      AgentProcess process = new AgentProcess(ServerConfiguration.global());
      process.awaitTermination();
    } catch (Exception e) {
      LogUtils.warnWithException(LOG, "hub agent server not terminated properly.", e);
      System.exit(1);
    }
    LOG.info("The hub agent is shut down now.");
    System.exit(0);
  }
}
