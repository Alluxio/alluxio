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
import alluxio.conf.Configuration;
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
public class WorkerMetaStoreTest {
  private static final String NOT_EXIST_PATH = "mNotExistPath";
  private static final String KEY1 = "key1";
  private static final String VALUE1 = "value1";
  private static final String KEY2 = "key2";
  private static final String VALUE2 = "value2";
  /**
   * Sets up all dependencies before a test runs.
   */
  WorkerMetaStore mWorkerMetaStore;

  /**
   * Rule to create a new temporary folder during each test.
   */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  void createDefault() throws IOException {
    // write permission is required,
    // The ALLUXIO_HOME directory in some test environment does not have write permission,
    // so the default WORKER_PERSISTENCE_INFO_PATH is set to temporary folder
    Configuration.set(PropertyKey.WORKER_METASTORE_PATH,
        mTestFolder.getRoot().getAbsolutePath());
    mWorkerMetaStore = WorkerMetaStore.Factory.create(Configuration.global());
    reset();
  }

  /**
   * Resets the worker persistence info to original state. not to do if persistence file not exist
   */
  void reset() {
    if (mWorkerMetaStore == null) {
      return;
    }
    try {
      mWorkerMetaStore.reset();
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  // Test for File-implemented metadata storage
  @Test
  public void fileCreate() throws IOException {
    Path tmpPath = Files.createTempDirectory("tmp-" + UUID.randomUUID());
    Configuration.set(PropertyKey.WORKER_METASTORE_PATH, tmpPath.toString());
    // If the DB file does not exist, it will be created
    Assert.assertFalse(Files.exists(Paths.get(tmpPath.toString(), CLUSTERID_FILE)));
    new DefaultWorkerMetaStore();
    Assert.assertTrue(Files.exists(Paths.get(tmpPath.toString(), CLUSTERID_FILE)));
  }

  @Test
  public void resetEmpty() throws IOException {
    mWorkerMetaStore =
        new DefaultWorkerMetaStore(
            PathUtils.concatPath(mTestFolder.getRoot().getAbsolutePath(), NOT_EXIST_PATH));
    // nothing to do if reset a not exist file
    mWorkerMetaStore.reset();
  }

  // Test for WorkerMetaStore interface
  @Test
  public void setAndGet() throws IOException {
    createDefault();
    mWorkerMetaStore.set(KEY1, VALUE1);
    Assert.assertEquals(VALUE1, mWorkerMetaStore.get(KEY1));
  }

  // Test for WorkerMetaStore interface
  @Test
  public void getNotExistKey() throws IOException {
    createDefault();
    Assert.assertNull(VALUE1, mWorkerMetaStore.get(KEY1));
  }

  @Test
  public void multiSetKey() throws IOException {
    createDefault();

    mWorkerMetaStore.set(KEY1, VALUE1);
    mWorkerMetaStore.set(KEY2, VALUE2);
    Assert.assertEquals(VALUE2, mWorkerMetaStore.get(KEY2));
  }

  @Test
  public void clusterIdHasBeenPersisted() throws IOException {
    Configuration.set(PropertyKey.WORKER_METASTORE_PATH,
        mTestFolder.getRoot().getAbsolutePath());
    WorkerMetaStore workerMetaStore1 = WorkerMetaStore.Factory.create(Configuration.global());
    workerMetaStore1.set(KEY1, VALUE1);
    // get from another Object
    WorkerMetaStore workerMetaStore2 = WorkerMetaStore.Factory.create(Configuration.global());
    Assert.assertEquals(VALUE1, workerMetaStore2.get(KEY1));
  }

  @Test
  public void resetMeta() throws IOException {
    createDefault();
    mWorkerMetaStore.set(KEY1, VALUE1);
    mWorkerMetaStore.reset();
    // after reset, all information will be cleared, so will get IdUtils.EMPTY_CLUSTER_ID
    Assert.assertNull(mWorkerMetaStore.get(KEY1));
  }

  @Test
  public void factoryCreateFromNoExistPath() throws IOException {
    String noExistPath = PathUtils.concatPath(
        mTestFolder.getRoot().getAbsolutePath(), "a", "b", "c");
    Configuration.set(PropertyKey.WORKER_METASTORE_PATH, noExistPath);
    // skip workerMetaStore.Factory.create "TEST_MODE"
    Configuration.set(PropertyKey.TEST_MODE, false);

    WorkerMetaStore workerMetaStore = WorkerMetaStore.Factory.create(Configuration.global());
    workerMetaStore.set(KEY1, VALUE1);
    Assert.assertTrue(Files.exists(Paths.get(noExistPath, CLUSTERID_FILE)));
    Assert.assertEquals(VALUE1, workerMetaStore.get(KEY1));
  }
}
