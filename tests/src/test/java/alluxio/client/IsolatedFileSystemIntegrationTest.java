/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.Configuration;
import alluxio.exception.AlluxioException;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

/**
 * Integration tests on TachyonClient (Do not reuse the LocalTachyonCluster).
 */
public class IsolatedFileSystemIntegrationTest {
  private static final int WORKER_CAPACITY_BYTES = 200 * Constants.MB;
  private static final int USER_QUOTA_UNIT_BYTES = 1000;
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource = new LocalAlluxioClusterResource(
      WORKER_CAPACITY_BYTES, 100 * Constants.MB,
      Constants.USER_FILE_BUFFER_BYTES, Integer.toString(USER_QUOTA_UNIT_BYTES));
  private FileSystem mFileSystem = null;
  private int mWorkerToMasterHeartbeatIntervalMs;
  private CreateFileOptions mWriteBoth;

  @Before
  public final void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();

    Configuration workerConfiguration = mLocalAlluxioClusterResource.get().getWorkerConf();
    mWorkerToMasterHeartbeatIntervalMs =
        workerConfiguration.getInt(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS);
    mWriteBoth = StreamOptionUtils.getCreateFileOptionsCacheThrough(workerConfiguration);
  }

  @Test
  public void lockBlockTest1() throws IOException, AlluxioException {
    String uniqPath = PathUtils.uniqPath();
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<AlluxioURI> files = new ArrayList<AlluxioURI>();
    for (int k = 0; k < numOfFiles; k ++) {
      FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + k, fileSize, mWriteBoth);
      files.add(new AlluxioURI(uniqPath + k));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      Assert.assertTrue(mFileSystem.getStatus(files.get(k)).getInMemoryPercentage() == 100);
    }
    FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + numOfFiles, fileSize, mWriteBoth);
    files.add(new AlluxioURI(uniqPath + numOfFiles));

    CommonUtils.sleepMs(mWorkerToMasterHeartbeatIntervalMs);

    Assert.assertFalse(mFileSystem.getStatus(files.get(0)).getInMemoryPercentage() == 100);
    for (int k = 1; k <= numOfFiles; k ++) {
      Assert.assertTrue(mFileSystem.getStatus(files.get(k)).getInMemoryPercentage() == 100);
    }
  }

  @Test
  public void lockBlockTest2() throws IOException, AlluxioException {
    String uniqPath = PathUtils.uniqPath();
    FileInStream is;
    ByteBuffer buf;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<AlluxioURI> files = new ArrayList<AlluxioURI>();
    for (int k = 0; k < numOfFiles; k ++) {
      FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + k, fileSize, mWriteBoth);
      files.add(new AlluxioURI(uniqPath + k));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      URIStatus info = mFileSystem.getStatus(files.get(k));
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
      is = mFileSystem.openFile(files.get(k), FileSystemTestUtils.toOpenFileOptions(mWriteBoth));
      buf = ByteBuffer.allocate((int) info.getBlockSizeBytes());
      Assert.assertTrue(is.read(buf.array()) != -1);
      is.close();
    }
    FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + numOfFiles, fileSize, mWriteBoth);
    files.add(new AlluxioURI(uniqPath + numOfFiles));

    for (int k = 1; k < numOfFiles; k ++) {
      URIStatus info = mFileSystem.getStatus(files.get(k));
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
    }
    // Sleep to ensure eviction has been reported to master
    CommonUtils.sleepMs(getSleepMs());
    URIStatus info = mFileSystem.getStatus(files.get(numOfFiles));
    Assert.assertTrue(info.getInMemoryPercentage() == 100);
  }

  @Test
  public void lockBlockTest3() throws IOException, AlluxioException {
    String uniqPath = PathUtils.uniqPath();
    FileInStream is;
    ByteBuffer buf;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<AlluxioURI> files = new ArrayList<AlluxioURI>();
    for (int k = 0; k < numOfFiles; k ++) {
      FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + k, fileSize, mWriteBoth);
      files.add(new AlluxioURI(uniqPath + k));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      URIStatus info = mFileSystem.getStatus(files.get(k));
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
      is = mFileSystem.openFile(files.get(k), FileSystemTestUtils.toOpenFileOptions(mWriteBoth));
      buf = ByteBuffer.allocate((int) info.getBlockSizeBytes());
      int r = is.read(buf.array());
      if (k < numOfFiles - 1) {
        Assert.assertTrue(r != -1);
      }
      is.close();
    }
    FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + numOfFiles, fileSize, mWriteBoth);
    files.add(new AlluxioURI(uniqPath + numOfFiles));
    // Sleep to ensure eviction has been reported to master
    CommonUtils.sleepMs(getSleepMs());
    URIStatus info = mFileSystem.getStatus(files.get(0));
    Assert.assertFalse(info.getInMemoryPercentage() == 100);
    for (int k = 1; k <= numOfFiles; k ++) {
      info = mFileSystem.getStatus(files.get(k));
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
    }
  }

  @Test
  public void unlockBlockTest1() throws IOException, AlluxioException {
    String uniqPath = PathUtils.uniqPath();
    FileInStream is;
    ByteBuffer buf;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<AlluxioURI> files = new ArrayList<AlluxioURI>();
    for (int k = 0; k < numOfFiles; k ++) {
      FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + k, fileSize, mWriteBoth);
      files.add(new AlluxioURI(uniqPath + k));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      URIStatus info = mFileSystem.getStatus(files.get(k));
      is = mFileSystem.openFile(files.get(k), FileSystemTestUtils.toOpenFileOptions(mWriteBoth));
      buf = ByteBuffer.allocate((int) info.getBlockSizeBytes());
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
      Assert.assertTrue(is.read(buf.array()) != -1);
      is.close();
    }
    FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + numOfFiles, fileSize, mWriteBoth);
    files.add(new AlluxioURI(uniqPath + numOfFiles));
    // Sleep to ensure eviction has been reported to master
    CommonUtils.sleepMs(getSleepMs());
    URIStatus info = mFileSystem.getStatus(files.get(0));
    Assert.assertFalse(info.getInMemoryPercentage() == 100);
    for (int k = 1; k <= numOfFiles; k ++) {
      URIStatus in = mFileSystem.getStatus(files.get(k));
      Assert.assertTrue(in.getInMemoryPercentage() == 100);
    }
  }

  @Test
  public void unlockBlockTest2() throws IOException, AlluxioException {
    String uniqPath = PathUtils.uniqPath();
    FileInStream is;
    ByteBuffer buf;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<AlluxioURI> files = new ArrayList<AlluxioURI>();
    for (int k = 0; k < numOfFiles; k ++) {
      FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + k, fileSize, mWriteBoth);
      files.add(new AlluxioURI(uniqPath + k));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      URIStatus info = mFileSystem.getStatus(files.get(k));
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
      is = mFileSystem.openFile(files.get(k), FileSystemTestUtils.toOpenFileOptions(mWriteBoth));
      buf = ByteBuffer.allocate((int) info.getBlockSizeBytes());
      Assert.assertTrue(is.read(buf.array()) != -1);
      is.seek(0);
      buf.clear();
      Assert.assertTrue(is.read(buf.array()) != -1);
      is.close();
    }
    FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + numOfFiles, fileSize, mWriteBoth);
    files.add(new AlluxioURI(uniqPath + numOfFiles));
    for (int k = 1; k < numOfFiles; k ++) {
      URIStatus info = mFileSystem.getStatus(files.get(k));
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
    }
    // Sleep to ensure eviction has been reported to master
    CommonUtils.sleepMs(getSleepMs());
    URIStatus info = mFileSystem.getStatus(files.get(numOfFiles));
    Assert.assertTrue(info.getInMemoryPercentage() == 100);
  }

  @Test
  public void unlockBlockTest3() throws IOException, AlluxioException {
    String uniqPath = PathUtils.uniqPath();
    FileInStream is;
    ByteBuffer buf1;
    ByteBuffer buf2;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<AlluxioURI> files = new ArrayList<AlluxioURI>();
    for (int k = 0; k < numOfFiles; k ++) {
      FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + k, fileSize, mWriteBoth);
      files.add(new AlluxioURI(uniqPath + k));
    }
    for (int k = 0; k < numOfFiles; k ++) {
      URIStatus info = mFileSystem.getStatus(files.get(k));
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
      is = mFileSystem.openFile(files.get(k), FileSystemTestUtils.toOpenFileOptions(mWriteBoth));
      buf1 = ByteBuffer.allocate((int) info.getBlockSizeBytes());
      Assert.assertTrue(is.read(buf1.array()) != -1);
      buf2 = ByteBuffer.allocate((int) info.getBlockSizeBytes());
      is.seek(0);
      Assert.assertTrue(is.read(buf2.array()) != -1);
      is.close();
    }
    FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + numOfFiles, fileSize, mWriteBoth);
    files.add(new AlluxioURI(uniqPath + numOfFiles));
    // Sleep to ensure eviction has been reported to master
    CommonUtils.sleepMs(getSleepMs());
    URIStatus info = mFileSystem.getStatus(files.get(0));
    Assert.assertFalse(info.getInMemoryPercentage() == 100);
    for (int k = 1; k <= numOfFiles; k ++) {
      URIStatus in = mFileSystem.getStatus(files.get(k));
      Assert.assertTrue(in.getInMemoryPercentage() == 100);
    }
  }

  private long getSleepMs() {
    return mWorkerToMasterHeartbeatIntervalMs * 2 + 10;
  }
}
