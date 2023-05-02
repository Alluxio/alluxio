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
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Optional;

/**
 * Test for RpcSimpleService.
 */
public class RpcServerServiceTest {
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
  public void primaryOnlyTest() {
    RpcServerService service =
        RpcServerService.Factory.create(mRpcAddress, mMasterProcess, mRegistry);
    Assert.assertTrue(waitForFree());

    Assert.assertFalse(service.isServing());
    service.start();
    // after start and before stop the rpc port is always bound as either the rpc server or the
    // rejecting server is bound to is (depending on whether it is in PRIMARY or STANDBY state)
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

  @Test
  public void doubleStartRejectingServer() {
    RpcServerService service =
        RpcServerService.Factory.create(mRpcAddress, mMasterProcess, mRegistry);

    service.start();
    Assert.assertThrows("rejecting server must not be running",
        IllegalStateException.class, service::start);
  }

  @Test
  public void doubleStartRpcServer() {
    RpcServerService service =
        RpcServerService.Factory.create(mRpcAddress, mMasterProcess, mRegistry);

    service.start();
    service.promote();
    Assert.assertThrows("rpc server must not be running",
        IllegalStateException.class, service::promote);
  }

  private boolean isBound() {
    try (Socket socket = new Socket(mRpcAddress.getAddress(), mRpcAddress.getPort())) {
      return true;
    } catch (ConnectException e) {
      return false;
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
