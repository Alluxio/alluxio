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

package alluxio.client.fs;

import static org.junit.Assert.assertEquals;

import alluxio.AlluxioTestDirectory;
import alluxio.ConfigurationRule;
import alluxio.ConfigurationTestUtils;
import alluxio.Process;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.WritePType;
import alluxio.master.AlluxioMasterProcess;
import alluxio.master.TestUtils;
import alluxio.network.TieredIdentityFactory;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.AlluxioWorkerProcess;
import alluxio.worker.WorkerProcess;
import alluxio.worker.block.BlockWorker;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Integration tests for functionality relating to tiered identity.
 */
public class LocalFirstPolicyIntegrationTest extends BaseIntegrationTest {
  private ExecutorService mExecutor;

  @Rule
  public ConfigurationRule mConf = new ConfigurationRule(conf(), ServerConfiguration.global());

  private static Map<PropertyKey, String> conf() {
    Map<PropertyKey, String> map =
        ConfigurationTestUtils.testConfigurationDefaults(ServerConfiguration.global(),
        NetworkAddressUtils.getLocalHostName(
            (int) ServerConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS)),
        AlluxioTestDirectory.createTemporaryDirectory("tiered_identity_test").getAbsolutePath());
    map.put(PropertyKey.WORKER_RPC_PORT, "0");
    map.put(PropertyKey.WORKER_WEB_PORT, "0");

    return map;
  }

  @Before
  public void before() {
    mExecutor = Executors.newCachedThreadPool();
  }

  @After
  public void after() {
    mExecutor.shutdownNow();
  }

  @Test
  public void test() throws Exception {
    AlluxioMasterProcess master = AlluxioMasterProcess.Factory.create();
    WorkerProcess worker1 = AlluxioWorkerProcess.Factory
        .create(TieredIdentityFactory.fromString("node=node1,rack=rack1",
            ServerConfiguration.global()));
    WorkerProcess worker2 = AlluxioWorkerProcess.Factory
        .create(TieredIdentityFactory.fromString("node=node2,rack=rack2",
            ServerConfiguration.global()));

    runProcess(mExecutor, master);
    runProcess(mExecutor, worker1);
    runProcess(mExecutor, worker2);

    TestUtils.waitForReady(master);
    TestUtils.waitForReady(worker1);
    TestUtils.waitForReady(worker2);

    FileSystem fs = FileSystem.Factory.create(ServerConfiguration.global());

    // Write to the worker in node1
    {
      Whitebox.setInternalState(TieredIdentityFactory.class, "sInstance",
          TieredIdentityFactory.fromString("node=node1,rack=rack1",
              ServerConfiguration.global()));
      try {
        FileSystemTestUtils.createByteFile(fs, "/file1", WritePType.MUST_CACHE, 100);
      } finally {
        Whitebox.setInternalState(TieredIdentityFactory.class, "sInstance", (Object) null);
      }
      BlockWorker blockWorker1 = worker1.getWorker(BlockWorker.class);
      BlockWorker blockWorker2 = worker2.getWorker(BlockWorker.class);
      assertEquals(100, blockWorker1.getStoreMeta().getUsedBytes());
      assertEquals(0, blockWorker2.getStoreMeta().getUsedBytes());
    }

    // Write to the worker in rack2
    {
      Whitebox.setInternalState(TieredIdentityFactory.class, "sInstance",
          TieredIdentityFactory.fromString("node=node3,rack=rack2",
              ServerConfiguration.global()));
      try {
        FileSystemTestUtils.createByteFile(fs, "/file2", WritePType.MUST_CACHE, 10);
      } finally {
        Whitebox.setInternalState(TieredIdentityFactory.class, "sInstance", (Object) null);
      }
      BlockWorker blockWorker1 = worker1.getWorker(BlockWorker.class);
      BlockWorker blockWorker2 = worker2.getWorker(BlockWorker.class);
      assertEquals(100, blockWorker1.getStoreMeta().getUsedBytes());
      assertEquals(10, blockWorker2.getStoreMeta().getUsedBytes());
    }
  }

  private void runProcess(ExecutorService e, Process p) {
    e.execute(() -> {
      try {
        p.start();
      } catch (Exception e1) {
        throw new RuntimeException(e1);
      }
    });
  }
}
