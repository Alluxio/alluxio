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

package alluxio.hub.manager.rpc.service;

import alluxio.RpcUtils;
import alluxio.hub.manager.process.ManagerProcessContext;
import alluxio.hub.proto.AgentHeartbeatRequest;
import alluxio.hub.proto.AgentHeartbeatResponse;
import alluxio.hub.proto.ManagerAgentServiceGrpc;
import alluxio.hub.proto.RegisterAgentRequest;
import alluxio.hub.proto.RegisterAgentResponse;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The implementation for the service of the Hub Manager manager which handles RPCs from agents.
 */
public class ManagerAgentService extends ManagerAgentServiceGrpc.ManagerAgentServiceImplBase {
  public static final Logger LOG = LoggerFactory.getLogger(ManagerAgentService.class);

  private final ManagerProcessContext mContext;

  /**
   * Creates an instance of the RPC handler for agent requests.
   *
   * @param ctx the process context
   */
  public ManagerAgentService(ManagerProcessContext ctx) {
    mContext = ctx;
  }

  @Override
  public void registerAgent(RegisterAgentRequest request,
      StreamObserver<RegisterAgentResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      Preconditions.checkArgument(request.hasNode());
      mContext.registerAgent(request);
      return RegisterAgentResponse.newBuilder().setOk(true).build();
    }, "registerAgent",  "registers a Hub agent with the Hub Manager", responseObserver, request);
  }

  @Override
  public void agentHeartbeat(AgentHeartbeatRequest request,
      StreamObserver<AgentHeartbeatResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      Preconditions.checkArgument(request.hasAlluxioStatus());
      Preconditions.checkArgument(request.hasHubNode());
      mContext.agentHearbeat(request);
      return AgentHeartbeatResponse.newBuilder().setOk(true).build();
    }, "agentHeartbeat",  "updates the Hub Manager with new information from the agent",
        responseObserver, request);
  }
}
