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

import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.conf.PropertyKey;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;

/**
 * Abstract classes for all integration tests of {@link FileOutStream}.
 */
public abstract class AbstractFileOutStreamIntegrationTest extends BaseIntegrationTest {
  protected static final int MIN_LEN = 0;
  protected static final int MAX_LEN = 255;
  protected static final int DELTA = 32;
  protected static final int BUFFER_BYTES = 512;
  protected static final int BLOCK_SIZE_BYTES = 1024;
  protected static LocalAlluxioJobCluster sLocalAlluxioJobCluster;

  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      buildLocalAlluxioClusterResource();

  @ClassRule
  public static RuleChain sRuleChain = RuleChain.outerRule(sLocalAlluxioClusterResource)
      .around(sLocalAlluxioClusterResource.getResetResource());

  protected static FileSystem sFileSystem = null;

  @BeforeClass
  public static void beforeClass() throws Exception {
    sLocalAlluxioJobCluster = new LocalAlluxioJobCluster();
    sLocalAlluxioJobCluster.setProperty(PropertyKey.JOB_MASTER_WORKER_HEARTBEAT_INTERVAL, "25ms");
    sLocalAlluxioJobCluster.start();
    sFileSystem = sLocalAlluxioClusterResource.get().getClient();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (sLocalAlluxioJobCluster != null) {
      sLocalAlluxioJobCluster.stop();
    }
    sFileSystem.close();
  }

  @Before
  public void before() throws Exception {
    sFileSystem = sLocalAlluxioClusterResource.get().getClient();
  }

  @After
  public void after() throws Exception {
    if (sFileSystem != null) {
      sFileSystem.close();
    }
  }

  /**
   * Override this method in a test in order to customize the {@link LocalAlluxioClusterResource}.
   * @param resource an AlluxioClusterResource builder
   */
  protected static void customizeClusterResource(LocalAlluxioClusterResource.Builder resource) {
    resource.setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, BUFFER_BYTES)
        .setProperty(PropertyKey.USER_FILE_REPLICATION_DURABLE, 1)
        .setProperty(PropertyKey.MASTER_PERSISTENCE_SCHEDULER_INTERVAL_MS, "100ms")
        .setProperty(PropertyKey.MASTER_PERSISTENCE_CHECKER_INTERVAL_MS, "100ms")
        .setProperty(PropertyKey.WORKER_TIERED_STORE_RESERVER_INTERVAL_MS, "100ms")
        .setProperty(PropertyKey.MASTER_WORKER_HEARTBEAT_INTERVAL, "100ms")
        .setProperty(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS, "100ms")
        .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE_BYTES);
  }

  private static LocalAlluxioClusterResource buildLocalAlluxioClusterResource() {
    LocalAlluxioClusterResource.Builder resource = new LocalAlluxioClusterResource.Builder();
    customizeClusterResource(resource);
    return resource.build();
  }
}
