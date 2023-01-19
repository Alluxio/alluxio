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

import alluxio.master.AlluxioMasterProcess;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test for RpcSimpleService.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(AlluxioMasterProcess.class)
public class RpcServerServiceTest extends RpcServerServiceTestBase {
  @Test
  public void primaryOnlyTest() {
    RpcServerService service =
        RpcServerService.Factory.create(mRpcAddress, mMasterProcess, mRegistry);
    Assert.assertTrue(waitForFree());

    Assert.assertFalse(service.isServing());
    service.start();
    // after start and before stop the rpc port is always bound as either the rpc server or the
    // rejecting server is bound to is (depending on whether it is in PRIMARY or STANDBY state)
    Assert.assertTrue(isGrpcBound());
    Assert.assertFalse(service.isServing());
    for (int i = 0; i < 5; i++) {
      service.promote();
      Assert.assertTrue(service.isServing());
      Assert.assertTrue(isGrpcBound());
      service.demote();
      Assert.assertTrue(isGrpcBound());
      Assert.assertFalse(service.isServing());
    }
    service.stop();
    Assert.assertFalse(service.isServing());
    Assert.assertFalse(isGrpcBound());
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
}
