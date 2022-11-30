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

import alluxio.conf.Configuration;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.master.AlluxioMasterProcess;
import alluxio.master.MasterRegistry;
import alluxio.master.PortReservationRule;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Optional;

/**
 * Test for RpcSimpleService.
 */
public class RpcServerSimpleServiceTest {
  @Rule
  public PortReservationRule mPort = new PortReservationRule();

  private final MasterRegistry mRegistry = new MasterRegistry();
  private InetSocketAddress mRpcAddress;
  private AlluxioMasterProcess mMasterProcess;

  @Before
  public void setUp() {
    mRpcAddress = new InetSocketAddress(mPort.getPort());
    mMasterProcess = Mockito.mock(AlluxioMasterProcess.class);
    Mockito.when(mMasterProcess.createBaseRpcServer()).thenAnswer(mock ->
        GrpcServerBuilder.forAddress(GrpcServerAddress.create(mRpcAddress.getHostName(),
            mRpcAddress), Configuration.global()));
    Mockito.when(mMasterProcess.createRpcExecutorService()).thenReturn(Optional.empty());
    Mockito.when(mMasterProcess.getSafeModeManager()).thenReturn(Optional.empty());
  }

  @Test
  public void whenLeadingTest() {
    RpcServerSimpleService service =
        RpcServerSimpleService.Factory.create(mRpcAddress, mMasterProcess, mRegistry);
    Assert.assertTrue(waitForFree());

    Assert.assertFalse(service.isServing());
    service.start();
    Assert.assertTrue(isBound());
    Assert.assertFalse(service.isServing());
    for (int i = 0; i < 5; i++) {
      service.promote();
      Assert.assertTrue(service.isServing());
      Assert.assertTrue(isBound());
      service.demote();
      Assert.assertTrue(isBound());
      Assert.assertFalse(service.isServing());
    }
    service.stop();
    Assert.assertFalse(service.isServing());
    Assert.assertFalse(isBound());
  }

  private boolean isBound() {
    try (ServerSocket socket = new ServerSocket(mRpcAddress.getPort())) {
      socket.setReuseAddress(true);
      return false;
    } catch (BindException e) {
      return true;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean waitForFree() {
    try {
      CommonUtils.waitFor("wait for socket to be free", () -> !isBound(),
          WaitForOptions.defaults().setTimeoutMs(1_000).setInterval(10));
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
