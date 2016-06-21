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

package alluxio.client;

import alluxio.AlluxioProcess;
import alluxio.Configuration;
import alluxio.Context;
import alluxio.client.util.ClientTestUtils;
import alluxio.exception.ExceptionMessage;
import alluxio.master.MasterContext;
import alluxio.worker.WorkerContext;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

/**
 * Tests for {@link alluxio.Context}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(Context.class)
public final class ContextTest {
  @Test
  public void clientContextTest() {
    resetContexts();
    AlluxioProcess.setType(AlluxioProcess.Type.CLIENT);
    ClientContext.getConf().set("test_key", "client");
    Assert.assertEquals(Context.getConf().get("test_key"), "client");
    try {
      MasterContext.getConf().get("test_key");
      Assert.fail();
    } catch (RuntimeException e) {
      Assert.assertEquals(e.getMessage(),
          ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage("test_key"));
    }
    try {
      WorkerContext.getConf().get("test_key");
      Assert.fail();
    } catch (RuntimeException e) {
      Assert.assertEquals(e.getMessage(),
          ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage("test_key"));
    }
  }

  @Test
  public void masterContextTest() {
    resetContexts();
    AlluxioProcess.setType(AlluxioProcess.Type.MASTER);
    MasterContext.getConf().set("test_key", "master");
    Assert.assertEquals(Context.getConf().get("test_key"), "master");
    try {
      ClientContext.getConf().get("test_key");
      Assert.fail();
    } catch (RuntimeException e) {
      Assert.assertEquals(e.getMessage(),
          ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage("test_key"));
    }
    try {
      WorkerContext.getConf().get("test_key");
      Assert.fail();
    } catch (RuntimeException e) {
      Assert.assertEquals(e.getMessage(),
          ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage("test_key"));
    }
  }

  @Test
  public void workerContextTest() {
    resetContexts();
    AlluxioProcess.setType(AlluxioProcess.Type.WORKER);
    WorkerContext.getConf().set("test_key", "worker");
    Assert.assertEquals(Context.getConf().get("test_key"), "worker");
    try {
      ClientContext.getConf().get("test_key");
      Assert.fail();
    } catch (RuntimeException e) {
      Assert.assertEquals(e.getMessage(),
          ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage("test_key"));
    }
    try {
      MasterContext.getConf().get("test_key");
      Assert.fail();
    } catch (RuntimeException e) {
      Assert.assertEquals(e.getMessage(),
          ExceptionMessage.INVALID_CONFIGURATION_KEY.getMessage("test_key"));
    }
  }

  private void resetContexts() {
    Whitebox.setInternalState(Context.class, "sConfiguration", (Configuration) null);
    ClientTestUtils.resetClientContext();
    MasterContext.reset();
    WorkerContext.reset();
  }
}
