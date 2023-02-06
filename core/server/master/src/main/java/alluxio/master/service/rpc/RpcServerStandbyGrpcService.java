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

package alluxio.master.service.rpc;

import alluxio.master.Master;
import alluxio.master.MasterProcess;
import alluxio.master.MasterRegistry;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * Created by {@link RpcServerService.Factory}.
 * Manages the behavior of the master's rpc service. The grpc server is always on.
 * When the promotion/demotion happens, the rpc service will be stopped and restarted.
 * The new started grpc service will serve gRPC endpoints based on the node state (PRIMARY/STANDBY).
 * No rejecting server is deployed.
 */
public class RpcServerStandbyGrpcService extends RpcServerService {
  protected static final Logger LOG = LoggerFactory.getLogger(RpcServerStandbyGrpcService.class);

  private boolean mIsPromoted = false;

  protected RpcServerStandbyGrpcService(
      InetSocketAddress bindAddress,
      MasterProcess masterProcess,
      MasterRegistry masterRegistry
  ) {
    super(bindAddress, masterProcess, masterRegistry);
  }

  @Override
  public synchronized void start() {
    LOG.info("Starting {}", this.getClass().getSimpleName());
    startGrpcServer(Master::getStandbyServices);
  }

  @Override
  public synchronized void stop() {
    stopGrpcServer();
    stopRpcExecutor();
    mIsPromoted = false;
  }

  @Override
  public synchronized void promote() {
    Preconditions.checkState(!mIsPromoted, "double promotion is not allowed");
    LOG.info("Promoting {}", this.getClass().getSimpleName());
    stopGrpcServer();
    stopRpcExecutor();
    waitForFree();
    startGrpcServer(Master::getServices);
    mIsPromoted = true;
  }

  @Override
  public synchronized void demote() {
    Preconditions.checkState(mIsPromoted, "double demotion is not allowed");
    LOG.info("Demoting {}", this.getClass().getSimpleName());
    stopGrpcServer();
    stopRpcExecutor();
    waitForFree();
    startGrpcServer(Master::getStandbyServices);
    mIsPromoted = false;
  }
}
