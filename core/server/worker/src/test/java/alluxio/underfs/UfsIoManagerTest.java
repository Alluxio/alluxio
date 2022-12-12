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

package alluxio.underfs;

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
import alluxio.grpc.UfsReadOptions;
import alluxio.underfs.UfsManager.UfsClient;
import alluxio.util.io.BufferUtils;

import com.codahale.metrics.Meter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

public final class UfsIoManagerTest {
  private static final long TEST_BLOCK_SIZE = 5 * Constants.MB;
  private static final long FIRST_BLOCK_ID = 0;
  private static final ByteBuffer TEST_BUF = ByteBuffer.allocate((int) TEST_BLOCK_SIZE);
  private UfsIOManager mUfsIOManager;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Rule
  public ConfigurationRule mConfigurationRule =
      new ConfigurationRule(new HashMap<PropertyKey, Object>() {
        {
          put(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS,
              AlluxioTestDirectory.createTemporaryDirectory(
                  "UnderFileSystemBlockReaderTest-RootUfs").getAbsolutePath());
        }
      }, Configuration.modifiableGlobal());
  private String mTestFilePath;

  @Before
  public void before() throws Exception {
    String ufsFolder = Configuration.getString(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    mTestFilePath = File.createTempFile("temp", null, new File(ufsFolder)).getAbsolutePath();
    byte[] buffer = BufferUtils.getIncreasingByteArray((int) TEST_BLOCK_SIZE * 2);
    BufferUtils.writeBufferToFile(mTestFilePath, buffer);
    UfsClient client = new UfsClient(() -> UnderFileSystem.Factory.create(mTestFilePath,
        UnderFileSystemConfiguration.defaults(Configuration.global())),
        new AlluxioURI(mTestFilePath));
    mUfsIOManager = spy(new UfsIOManager(client));
    doReturn(3d * Constants.MB).when(mUfsIOManager).getUsedThroughput(any(Meter.class));
    mUfsIOManager.start();
  }

  @Test
  public void readFullBlock() throws Exception {
    mUfsIOManager.read(TEST_BUF, 0, TEST_BLOCK_SIZE, FIRST_BLOCK_ID, mTestFilePath,
        UfsReadOptions.getDefaultInstance()).get();
    assertTrue(checkBuf(0, (int) TEST_BLOCK_SIZE, TEST_BUF));
    TEST_BUF.clear();
  }

  @Test
  public void readPartialBlock() throws Exception {

    mUfsIOManager.read(TEST_BUF, 0, TEST_BLOCK_SIZE - 1, FIRST_BLOCK_ID, mTestFilePath,
        UfsReadOptions.getDefaultInstance()).get();
    assertTrue(checkBuf(0, (int) TEST_BLOCK_SIZE - 1, TEST_BUF));
    TEST_BUF.clear();
  }

  @Test
  public void readSecondBlock() throws Exception {
    mUfsIOManager.read(TEST_BUF, TEST_BLOCK_SIZE, TEST_BLOCK_SIZE, 1, mTestFilePath,
        UfsReadOptions.getDefaultInstance()).get();
    assertTrue(checkBuf((int) TEST_BLOCK_SIZE, (int) TEST_BLOCK_SIZE, TEST_BUF));
    TEST_BUF.clear();
  }

  @Test
  public void offset() throws Exception {
    mUfsIOManager.read(TEST_BUF, 2, TEST_BLOCK_SIZE - 2, FIRST_BLOCK_ID, mTestFilePath,
        UfsReadOptions.getDefaultInstance()).get();
    assertTrue(checkBuf(2, (int) TEST_BLOCK_SIZE - 2, TEST_BUF));
    TEST_BUF.clear();
  }

  @Test
  public void readOverlap() throws Exception {
    mUfsIOManager.read(TEST_BUF, 2, TEST_BLOCK_SIZE - 2, FIRST_BLOCK_ID, mTestFilePath,
        UfsReadOptions.getDefaultInstance()).get();
    assertTrue(checkBuf(2, (int) TEST_BLOCK_SIZE - 2, TEST_BUF));
    TEST_BUF.clear();
    mUfsIOManager.read(TEST_BUF, TEST_BLOCK_SIZE, TEST_BLOCK_SIZE, 1, mTestFilePath,
        UfsReadOptions.getDefaultInstance()).get();
    assertTrue(checkBuf((int) TEST_BLOCK_SIZE, (int) TEST_BLOCK_SIZE, TEST_BUF));
    TEST_BUF.clear();
    mUfsIOManager.read(TEST_BUF, 2 + TEST_BLOCK_SIZE, TEST_BLOCK_SIZE - 2, 1, mTestFilePath,
        UfsReadOptions.getDefaultInstance()).get();
    assertTrue(checkBuf((int) TEST_BLOCK_SIZE + 2, (int) TEST_BLOCK_SIZE - 2, TEST_BUF));
    TEST_BUF.clear();
  }

  @Test
  public void readWithQuota() throws Exception {
    mUfsIOManager.setQuota("quotaTest", 2 * Constants.MB); // magic number for proper test time
    CompletableFuture<Integer> future =
        mUfsIOManager.read(TEST_BUF, 2, TEST_BLOCK_SIZE - 2, FIRST_BLOCK_ID, mTestFilePath,
            UfsReadOptions.newBuilder().setTag("quotaTest").setPositionShort(false).build());
    // sleep to make sure future is not done because of quota instead of get future too soon
    Thread.sleep(3000);
    assertFalse(future.isDone());
    mUfsIOManager.setQuota("quotaTest", 5 * Constants.MB);
    future.get();
    assertTrue(checkBuf(2, (int) TEST_BLOCK_SIZE - 2, TEST_BUF));
    TEST_BUF.clear();
  }

  private boolean checkBuf(int start, int len, ByteBuffer buf) {
    buf.rewind();
    for (int k = 0; k < len; k++) {
      if (buf.get() != (byte) (start + k)) {
        return false;
      }
    }
    return true;
  }
}
