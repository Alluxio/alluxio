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

import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.IdUtils;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class BlockWorkerDBTest {
  /**
   * Sets up all dependencies before a test runs.
   */
  DefaultBlockWorkerDB mBlockWorkerDB;
  final String mNotExistPath = "mNotExistPath";
  final String mDBfile = "mDBfile";
  final String mKey1 = "key1";
  final String mValue1 = "value1";

  /**
   * Rule to create a new temporary folder during each test.
   */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  void createANotExistPath() {
    mBlockWorkerDB =
        new DefaultBlockWorkerDB(PathUtils.concatPath(mTestFolder.getRoot().getAbsolutePath(),
            mNotExistPath, "clusterid"));
    reset();
  }

  void createDefault() {
    InstancedConfiguration conf = ConfigurationTestUtils.defaults();
    conf.set(PropertyKey.WORKER_PERSISTENCE_INFO_PATH, mTestFolder.getRoot().getAbsolutePath());
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
      mBlockWorkerDB.resetState();
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testGetINVALIDClusterIdFromDB() {
    createDefault();
    Assert.assertEquals(IdUtils.INVALID_CLUSTER_ID, mBlockWorkerDB.getClusterId());
  }

  @Test
  public void testSetAndGetClusterIdFromDB() throws IOException {
    createDefault();
    String clusterId = java.util.UUID.randomUUID().toString();
    mBlockWorkerDB.setClusterId(clusterId);
    Assert.assertEquals(clusterId, mBlockWorkerDB.getClusterId());
  }

  @Test
  public void testSetAndGetToExistPath() throws IOException {
    createDefault();
    mBlockWorkerDB.set(mKey1, mValue1);
    Assert.assertEquals(mValue1, mBlockWorkerDB.get(mKey1));
  }

  @Test
  public void testSetAndGetToNotExistPath() throws IOException {
    createANotExistPath();
    // When writing info to a file that does not exist, the file will be created
    mBlockWorkerDB.set(mKey1, mValue1);
    Assert.assertEquals(mValue1, mBlockWorkerDB.get(mKey1));
  }

  @Test
  public void testGetToNotExistPath() throws IOException {
    createANotExistPath();
    // The get will return an empty string rather than a null pointer if the key does not exist
    Assert.assertEquals("", mBlockWorkerDB.get(mKey1));
  }

  @Test
  public void testResetStateExistPath() throws IOException {
    createDefault();
    mBlockWorkerDB.set(mKey1, mValue1);
    mBlockWorkerDB.resetState();
    // normal resetState, all info will be clear
    Assert.assertEquals("", mBlockWorkerDB.get(mKey1));
  }

  @Test
  public void testResetStateNotExistPath() throws IOException {
    mBlockWorkerDB =
        new DefaultBlockWorkerDB(
            PathUtils.concatPath(mTestFolder.getRoot().getAbsolutePath(), mNotExistPath));
    // nothing to do if reset a not exist file
    mBlockWorkerDB.resetState();
  }
}
