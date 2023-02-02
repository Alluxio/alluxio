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
import alluxio.conf.PropertyKey;
import alluxio.master.AlluxioMasterProcess;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test for RpcServerStandbyGrpcServiceTest.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(AlluxioMasterProcess.class)
public class RpcServerStandbyGrpcServiceTest extends RpcServerServiceTestBase {
  @Before
  public void setUp() {
    Configuration.reloadProperties();
    Configuration.set(PropertyKey.STANDBY_MASTER_GRPC_ENABLED, true);
    super.setUp();
  }

  @Test
  public void primaryOnlyTest() {
    RpcServerService service =
        RpcServerService.Factory.create(mRpcAddress, mMasterProcess, mRegistry);
    Assert.assertTrue(waitForFree());

    Assert.assertFalse(service.isServing());
    service.start();
    // when standby master is enabled, gRPC server is always on even if it's standby.
    Assert.assertTrue(isGrpcBound());
    Assert.assertTrue(service.isServing());
    for (int i = 0; i < 5; i++) {
      service.promote();
      Assert.assertTrue(service.isServing());
      Assert.assertTrue(isGrpcBound());
      service.demote();
      Assert.assertTrue(isGrpcBound());
      Assert.assertTrue(service.isServing());
    }
    service.stop();
    Assert.assertFalse(service.isServing());
    Assert.assertFalse(isGrpcBound());
  }

  @Test
  public void doubleStartRpcServer() {
    RpcServerService service =
        RpcServerService.Factory.create(mRpcAddress, mMasterProcess, mRegistry);

    service.start();
    service.promote();
    Assert.assertThrows("double promotion is not allowed",
        IllegalStateException.class, service::promote);

    service.demote();
    Assert.assertThrows("double demotion is not allowed",
        IllegalStateException.class, service::demote);
  }
}
