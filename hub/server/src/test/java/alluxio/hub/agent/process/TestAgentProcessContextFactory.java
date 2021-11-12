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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.conf.AlluxioConfiguration;
import alluxio.hub.agent.util.process.ProcessLauncher;
import alluxio.hub.common.RpcClient;
import alluxio.hub.proto.AlluxioNodeType;
import alluxio.hub.proto.ManagerAgentServiceGrpc;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TestAgentProcessContextFactory {

  public static AgentProcessContext newContext(Set<AlluxioNodeType> procs,
      ProcessLauncher launcher, AlluxioConfiguration conf) throws Exception {
    RpcClient<ManagerAgentServiceGrpc.ManagerAgentServiceBlockingStub> rpcClient =
        mock(RpcClient.class);
    ManagerAgentServiceGrpc.ManagerAgentServiceBlockingStub stub =
        mock(ManagerAgentServiceGrpc.ManagerAgentServiceBlockingStub.class);
    when(rpcClient.get()).thenReturn(stub);
    Map<AlluxioNodeType, Object> p = procs.stream()
        .collect(Collectors.toMap(s -> s, s -> new Object()));
    AgentProcessContext apc = new AgentProcessContext(conf, rpcClient, p, launcher);
    return apc;
  }

  public static AgentProcessContext simpleContext(AlluxioConfiguration conf) throws Exception {
    return newContext(Collections.singleton(AlluxioNodeType.MASTER), mock(ProcessLauncher.class),
        conf);
  }
}
