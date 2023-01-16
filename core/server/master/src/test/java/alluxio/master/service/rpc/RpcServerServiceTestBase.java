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

import org.junit.Before;
import org.junit.Rule;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Optional;

/**
 * Test base RpcService related tests.
 */
public class RpcServerServiceTestBase {
  @Rule
  public PortReservationRule mPort = new PortReservationRule();

  protected final MasterRegistry mRegistry = new MasterRegistry();
  protected InetSocketAddress mRpcAddress;
  protected AlluxioMasterProcess mMasterProcess;

  @Before
  public void setUp() {
    mRpcAddress = new InetSocketAddress(mPort.getPort());
    mMasterProcess = PowerMockito.mock(AlluxioMasterProcess.class);
    Mockito.when(mMasterProcess.createBaseRpcServer()).thenAnswer(mock ->
        GrpcServerBuilder.forAddress(GrpcServerAddress.create(mRpcAddress.getHostName(),
            mRpcAddress), Configuration.global()));
    Mockito.when(mMasterProcess.createRpcExecutorService()).thenReturn(Optional.empty());
    Mockito.when(mMasterProcess.getSafeModeManager()).thenReturn(Optional.empty());
  }

  protected boolean isGrpcBound() {
    return isBound(mRpcAddress);
  }

  protected boolean isBound(InetSocketAddress address) {
    try (Socket socket = new Socket(address.getAddress(), address.getPort())) {
      return true;
    } catch (ConnectException e) {
      return false;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected boolean waitForFree() {
    try {
      CommonUtils.waitFor("wait for socket to be free", () -> !isGrpcBound(),
          WaitForOptions.defaults().setTimeoutMs(1_000).setInterval(10));
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
