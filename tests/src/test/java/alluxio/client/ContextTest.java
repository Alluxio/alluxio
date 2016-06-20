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
import alluxio.Context;
import alluxio.master.MasterContext;
import alluxio.worker.WorkerContext;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link alluxio.Context}.
 */
public class ContextTest {
  @Test
  public void clientContextTest() {
    AlluxioProcess.setType(AlluxioProcess.Type.CLIENT);
    ClientContext.getConf().set("test_key", "client");
    Assert.assertEquals(Context.getConf().get("test_key"), "client");
  }

  @Test
  public void masterContextTest() {
    AlluxioProcess.setType(AlluxioProcess.Type.MASTER);
    MasterContext.getConf().set("test_key", "master");
    Assert.assertEquals(Context.getConf().get("test_key"), "master");
  }

  @Test
  public void workerContextTest() {
    AlluxioProcess.setType(AlluxioProcess.Type.WORKER);
    WorkerContext.getConf().set("test_key", "worker");
    Assert.assertEquals(Context.getConf().get("test_key"), "worker");
  }
}
