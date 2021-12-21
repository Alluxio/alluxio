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

package alluxio.worker.block;

import static alluxio.Constants.CLUSTERID_FILE;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.util.IdUtils;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

@RunWith(PowerMockRunner.class)
public class BlockWorkerDBTest {
  /**
   * Sets up all dependencies before a test runs.
   */
  BlockWorkerDB mBlockWorkerDB;
  final String mNotExistPath = "mNotExistPath";

  /**
   * Rule to create a new temporary folder during each test.
   */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  void createANotExistPath() throws IOException {
    mBlockWorkerDB =
        new DefaultBlockWorkerDB(PathUtils.concatPath(mTestFolder.getRoot().getAbsolutePath(),
            mNotExistPath, "clusterid"));
    reset();
  }

  void createDefault() throws IOException {
    // write permission is required,
    // The ALLUXIO_HOME directory in some test environment does not have write permission,
    // so the default WORKER_PERSISTENCE_INFO_PATH is set to temporary folder
    ServerConfiguration.set(PropertyKey.WORKER_CLUSTERID_PATH,
        mTestFolder.getRoot().getAbsolutePath());
    mBlockWorkerDB = new DefaultBlockWorkerDB();
    reset();
  }

  /**
   * Resets the worker persistence info to original state. not to do if persistence file not exist
   */
  void reset() {
    if (mBlockWorkerDB == null) {
      return;
    }
    try {
      mBlockWorkerDB.reset();
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testDBFileCreate() throws IOException {
    Path tmpPath = Files.createTempDirectory("tmp-" + UUID.randomUUID());
    ServerConfiguration.set(PropertyKey.WORKER_CLUSTERID_PATH, tmpPath.toString());
    // If the DB file does not exist, it will be created
    Assert.assertFalse(Files.exists(Paths.get(tmpPath.toString(), CLUSTERID_FILE)));
    new DefaultBlockWorkerDB();
    Assert.assertTrue(Files.exists(Paths.get(tmpPath.toString(), CLUSTERID_FILE)));
  }

  @Test
  public void testGetClusterIdFromEmptyDB() throws IOException {
    createDefault();
    // Do not set any information in DB, will be get IdUtils.EMPTY_CLUSTER_ID
    Assert.assertEquals(IdUtils.EMPTY_CLUSTER_ID, mBlockWorkerDB.getClusterId());
  }

  @Test
  public void testSetClusterIdAndGetClusterId() throws IOException {
    createDefault();
    String clusterId = java.util.UUID.randomUUID().toString();
    mBlockWorkerDB.setClusterId(clusterId);
    Assert.assertEquals(clusterId, mBlockWorkerDB.getClusterId());
  }

  @Test
  public void testReSetClusterId() throws IOException {
    createDefault();

    String clusterId1 = java.util.UUID.randomUUID().toString();
    String clusterId2 = java.util.UUID.randomUUID().toString();
    mBlockWorkerDB.setClusterId(clusterId1);
    mBlockWorkerDB.setClusterId(clusterId2);
    Assert.assertEquals(clusterId2, mBlockWorkerDB.getClusterId());
  }

  @Test
  public void testClusterIdHasBeenPersisted() throws IOException {
    ServerConfiguration.set(PropertyKey.WORKER_CLUSTERID_PATH,
        mTestFolder.getRoot().getAbsolutePath());
    BlockWorkerDB blockWorkerDB1 = new DefaultBlockWorkerDB();
    String clusterId = java.util.UUID.randomUUID().toString();
    blockWorkerDB1.setClusterId(clusterId);

    BlockWorkerDB blockWorkerDB2 = new DefaultBlockWorkerDB();
    Assert.assertEquals(clusterId, blockWorkerDB2.getClusterId());
  }

  @Test
  public void testReset() throws IOException {
    createDefault();
    String clusterId = java.util.UUID.randomUUID().toString();
    mBlockWorkerDB.setClusterId(clusterId);
    mBlockWorkerDB.reset();
    // after reset, all information will be cleared, so will get IdUtils.EMPTY_CLUSTER_ID
    Assert.assertEquals(IdUtils.EMPTY_CLUSTER_ID, mBlockWorkerDB.getClusterId());
  }

  @Test
  public void testResetEmptyDB() throws IOException {
    mBlockWorkerDB =
        new DefaultBlockWorkerDB(
            PathUtils.concatPath(mTestFolder.getRoot().getAbsolutePath(), mNotExistPath));
    // nothing to do if reset a not exist file
    mBlockWorkerDB.reset();
  }

  @Test
  public void testFactoryCreate() throws IOException {
    BlockWorkerDB blockWorkerDB = BlockWorkerDB.Factory.create(ServerConfiguration.global());
    String clusterId = java.util.UUID.randomUUID().toString();
    blockWorkerDB.setClusterId(clusterId);
    Assert.assertEquals(clusterId, blockWorkerDB.getClusterId());
  }

  @Test
  public void testFactoryCreateFromNoExistPath() throws IOException {
    String noExistPath = PathUtils.concatPath(
        mTestFolder.getRoot().getAbsolutePath(), "a", "b", "c");
    ServerConfiguration.set(PropertyKey.WORKER_CLUSTERID_PATH, noExistPath);
    // skip BlockWorkerDB.Factory.create "TEST_MODE"
    ServerConfiguration.set(PropertyKey.TEST_MODE, "false");

    BlockWorkerDB blockWorkerDB = BlockWorkerDB.Factory.create(ServerConfiguration.global());
    String clusterId = java.util.UUID.randomUUID().toString();
    blockWorkerDB.setClusterId(clusterId);
    Assert.assertTrue(Files.exists(Paths.get(noExistPath, CLUSTERID_FILE)));
    Assert.assertEquals(clusterId, blockWorkerDB.getClusterId());
  }
}
