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

package alluxio.hub.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import alluxio.conf.InstancedConfiguration;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.GrpcChannel;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.hub.proto.ManagerAgentServiceGrpc;
import alluxio.hub.test.BaseHubTest;
import alluxio.retry.CountingRetry;

import io.grpc.Status;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.internal.verification.VerificationModeFactory;

import java.net.InetSocketAddress;

public class RpcClientTest extends BaseHubTest {

  InstancedConfiguration mConf;
  InetSocketAddress mAddr;
  GrpcChannelBuilder mMockChannelBuilder;

  @Before
  public void before() throws Exception {
    mConf = getTestConfig();
    mAddr = mock(InetSocketAddress.class);
    mMockChannelBuilder = mock(GrpcChannelBuilder.class);
    doReturn(mMockChannelBuilder).when(mMockChannelBuilder).setSubject(any());
  }

  @Test
  public void testGrpcChannelBuilderThrowsException() throws Exception {
    try (MockedStatic<GrpcChannelBuilder> builderMock =
      Mockito.mockStatic(GrpcChannelBuilder.class)) {
      builderMock.when(() -> GrpcChannelBuilder.newBuilder(any(), any()))
          .thenReturn(mMockChannelBuilder);
      doThrow(AlluxioStatusException.from(Status.UNAVAILABLE)).when(mMockChannelBuilder).build();
      RpcClient<ManagerAgentServiceGrpc.ManagerAgentServiceBlockingStub> client =
          new RpcClient<>(mConf, mAddr, ManagerAgentServiceGrpc::newBlockingStub,
              () -> new CountingRetry(4));
      assertThrows(AlluxioStatusException.class, client::get);
      verify(mMockChannelBuilder, VerificationModeFactory.times(5)).build();
    }
  }

  @Test
  public void testGetAddr() {
    assertEquals(mAddr, new RpcClient<>(mAddr, ManagerAgentServiceGrpc::newBlockingStub,
        (addr) -> mock(GrpcChannel.class)).getAddress());
  }

  @Test
  public void testUnhealthyChannel() throws Exception {
    GrpcChannel mockChannel = mock(GrpcChannel.class);
    doReturn(false).when(mockChannel).isShutdown();
    doReturn(false).when(mockChannel).isHealthy();
    RpcClient<ManagerAgentServiceGrpc.ManagerAgentServiceBlockingStub> client =
        new RpcClient<>(mAddr, ManagerAgentServiceGrpc::newBlockingStub,
            (addr) -> mockChannel);
    client.get();
    client.get();
    verify(mockChannel).shutdown();
  }

  @Test
  public void testClientClose() throws Exception {
    GrpcChannel mockChannel = mock(GrpcChannel.class);
    RpcClient<ManagerAgentServiceGrpc.ManagerAgentServiceBlockingStub> client =
        new RpcClient<>(mAddr, ManagerAgentServiceGrpc::newBlockingStub,
            (addr) -> mockChannel);
    client.get();
    client.close();
    verify(mockChannel).shutdown();
  }
}
