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

package alluxio.master;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.journal.noop.NoopJournalSystem;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.BindException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.TimeoutException;

/**
 * Tests for {@link AlluxioMasterProcess}.
 */
public final class AlluxioMasterProcessTest {

  @Rule
  public PortReservationRule mRpcPortRule = new PortReservationRule();
  @Rule
  public PortReservationRule mWebPortRule = new PortReservationRule();

  private int mRpcPort;
  private int mWebPort;

  @Before
  public void before() {
    mRpcPort = mRpcPortRule.getPort();
    mWebPort = mWebPortRule.getPort();
    ServerConfiguration.set(PropertyKey.MASTER_RPC_PORT, mRpcPort);
    ServerConfiguration.set(PropertyKey.MASTER_WEB_PORT, mWebPort);
  }

  @Test
  public void startStopPrimary() throws Exception {
    AlluxioMasterProcess master = new AlluxioMasterProcess(new NoopJournalSystem());
    Thread t = new Thread(() -> {
      try {
        master.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    t.start();
    startStopTest(master);
  }

  @Test
  public void startStopSecondary() throws Exception {
    FaultTolerantAlluxioMasterProcess master = new FaultTolerantAlluxioMasterProcess(
        new NoopJournalSystem(), new AlwaysSecondaryPrimarySelector());
    Thread t = new Thread(() -> {
      try {
        master.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    t.start();
    startStopTest(master);
  }

  private void startStopTest(AlluxioMasterProcess master) throws Exception {
    waitForServing(ServiceType.MASTER_RPC);
    waitForServing(ServiceType.MASTER_WEB);
    assertTrue(isBound(mRpcPort));
    assertTrue(isBound(mWebPort));
    master.stop();
    assertFalse(isBound(mRpcPort));
    assertFalse(isBound(mWebPort));
  }

  private void waitForServing(ServiceType service) throws TimeoutException, InterruptedException {
    InetSocketAddress addr =
        NetworkAddressUtils.getBindAddress(service, ServerConfiguration.global());
    CommonUtils.waitFor(service + " to be serving", () -> {
      try {
        Socket s = new Socket(addr.getAddress(), addr.getPort());
        s.close();
        return true;
      } catch (ConnectException e) {
        return false;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private boolean isBound(int port) {
    try {
      ServerSocket socket = new ServerSocket(port);
      socket.setReuseAddress(true);
      socket.close();
      return false;
    } catch (BindException e) {
      return true;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
