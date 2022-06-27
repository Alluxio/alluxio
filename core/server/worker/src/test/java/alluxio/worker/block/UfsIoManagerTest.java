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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UfsManager.UfsClient;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.io.BufferUtils;

import com.codahale.metrics.Meter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

public final class UfsIoManagerTest {
  private static final long TEST_BLOCK_SIZE = 5 * Constants.MB;
  private static final long FIRST_BLOCK_ID = 0;

  private UfsIOManager mUfsIOManager;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Rule
  public ConfigurationRule mConfigurationRule =
      new ConfigurationRule(new HashMap<PropertyKey, Object>() {
        {
          put(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS,
              AlluxioTestDirectory
                  .createTemporaryDirectory("UnderFileSystemBlockReaderTest-RootUfs")
                  .getAbsolutePath());
        }
      }, Configuration.modifiableGlobal());
  private String mTestFilePath;

  @Before
  public void before() throws Exception {
    String ufsFolder = Configuration.getString(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    mTestFilePath = File.createTempFile("temp", null, new File(ufsFolder)).getAbsolutePath();
    byte[] buffer = BufferUtils.getIncreasingByteArray((int) TEST_BLOCK_SIZE * 2);
    BufferUtils.writeBufferToFile(mTestFilePath, buffer);
    UfsClient client = new UfsClient(
        () -> UnderFileSystem.Factory.create(mTestFilePath,
            UnderFileSystemConfiguration.defaults(Configuration.global())),
        new AlluxioURI(mTestFilePath));
    mUfsIOManager = spy(new UfsIOManager(client));
    doReturn(3d * Constants.MB).when(mUfsIOManager).getUsedThroughput(any(Meter.class));
    mUfsIOManager.start();
  }

  @Test
  public void readFullBlock() throws Exception {
    CompletableFuture<byte[]> data = mUfsIOManager.read(FIRST_BLOCK_ID, 0, TEST_BLOCK_SIZE,
        TEST_BLOCK_SIZE,
        mTestFilePath, false, "test");
    assertTrue(BufferUtils.equalIncreasingByteArray(0, (int) TEST_BLOCK_SIZE, data.get()));
  }

  @Test
  public void readPartialBlock() throws Exception {
    CompletableFuture<byte[]> data = mUfsIOManager.read(FIRST_BLOCK_ID, 0, TEST_BLOCK_SIZE - 1,
        TEST_BLOCK_SIZE,
        mTestFilePath, false, "test");
    assertTrue(BufferUtils.equalIncreasingByteArray(0, (int) TEST_BLOCK_SIZE - 1, data.get()));
  }

  @Test
  public void readSecondBlock() throws Exception {
    CompletableFuture<byte[]> data =
        mUfsIOManager.read(1, 0, TEST_BLOCK_SIZE, TEST_BLOCK_SIZE, mTestFilePath, false, "test");
    assertTrue(BufferUtils.equalIncreasingByteArray((int) TEST_BLOCK_SIZE, (int) TEST_BLOCK_SIZE,
        data.get()));
  }

  @Test
  public void offset() throws Exception {
    mUfsIOManager.start();
    CompletableFuture<byte[]> data = mUfsIOManager.read(FIRST_BLOCK_ID, 2, TEST_BLOCK_SIZE - 2,
        TEST_BLOCK_SIZE,
        mTestFilePath, false, "test");
    assertTrue(BufferUtils.equalIncreasingByteArray(2, (int) TEST_BLOCK_SIZE - 2, data.get()));
  }

  @Test
  public void readOverlap() throws Exception {
    CompletableFuture<byte[]> data = mUfsIOManager.read(FIRST_BLOCK_ID, 2, TEST_BLOCK_SIZE - 2,
        TEST_BLOCK_SIZE,
        mTestFilePath, false, "test");
    CompletableFuture<byte[]> data2 =
        mUfsIOManager.read(1, 0, TEST_BLOCK_SIZE, TEST_BLOCK_SIZE, mTestFilePath, false, "test");
    CompletableFuture<byte[]> data3 =
        mUfsIOManager.read(1, 2, TEST_BLOCK_SIZE - 2, TEST_BLOCK_SIZE, mTestFilePath, false,
            "test");

    assertTrue(BufferUtils.equalIncreasingByteArray(2, (int) TEST_BLOCK_SIZE - 2, data.get()));
    assertTrue(BufferUtils.equalIncreasingByteArray((int) TEST_BLOCK_SIZE, (int) TEST_BLOCK_SIZE,
        data2.get()));
    assertTrue(BufferUtils.equalIncreasingByteArray((int) TEST_BLOCK_SIZE + 2,
        (int) TEST_BLOCK_SIZE - 2, data3.get()));
  }

  @Test
  public void readWithQuota() throws Exception {
    mUfsIOManager.setQuotaInMB("quotaTest", 2); // magic number for proper test time
    CompletableFuture<byte[]> data = mUfsIOManager.read(FIRST_BLOCK_ID, 2, TEST_BLOCK_SIZE - 2,
        TEST_BLOCK_SIZE,
        mTestFilePath, false, "quotaTest");
    // sleep to make sure future is not done because of quota instead of get future too soon
    Thread.sleep(3000);
    assertFalse(data.isDone());
    mUfsIOManager.setQuotaInMB("quotaTest", 5);
    assertTrue(BufferUtils.equalIncreasingByteArray(2, (int) TEST_BLOCK_SIZE - 2, data.get()));
  }
}
