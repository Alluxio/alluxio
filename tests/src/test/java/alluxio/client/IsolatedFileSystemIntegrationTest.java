/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.LocalAlluxioCluster;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Integration tests on Alluxio client (do not reuse the {@link LocalAlluxioCluster}).
 */
public class IsolatedFileSystemIntegrationTest {
  private static final int WORKER_CAPACITY_BYTES = 200 * Constants.MB;
  private static final int USER_QUOTA_UNIT_BYTES = 1000;

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule =
      new ManuallyScheduleHeartbeat(HeartbeatContext.WORKER_BLOCK_SYNC);

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource = new LocalAlluxioClusterResource(
      WORKER_CAPACITY_BYTES, 100 * Constants.MB,
      Constants.USER_FILE_BUFFER_BYTES, Integer.toString(USER_QUOTA_UNIT_BYTES));
  private FileSystem mFileSystem = null;
  private CreateFileOptions mWriteBoth;

  @Before
  public final void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    mWriteBoth = StreamOptionUtils.getCreateFileOptionsCacheThrough();
  }

  @Test
  public void lockBlockTest1() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<AlluxioURI> files = new ArrayList<AlluxioURI>();
    for (int k = 0; k < numOfFiles; k++) {
      FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + k, fileSize, mWriteBoth);
      files.add(new AlluxioURI(uniqPath + k));
    }
    for (int k = 0; k < numOfFiles; k++) {
      Assert.assertTrue(mFileSystem.getStatus(files.get(k)).getInMemoryPercentage() == 100);
    }
    FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + numOfFiles, fileSize, mWriteBoth);
    files.add(new AlluxioURI(uniqPath + numOfFiles));

    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 10,
        TimeUnit.SECONDS));
    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_BLOCK_SYNC);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 10,
        TimeUnit.SECONDS));

    Assert.assertFalse(mFileSystem.getStatus(files.get(0)).getInMemoryPercentage() == 100);
    for (int k = 1; k <= numOfFiles; k++) {
      Assert.assertTrue(mFileSystem.getStatus(files.get(k)).getInMemoryPercentage() == 100);
    }
  }

  @Test
  public void lockBlockTest2() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    FileInStream is;
    ByteBuffer buf;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<AlluxioURI> files = new ArrayList<AlluxioURI>();
    for (int k = 0; k < numOfFiles; k++) {
      FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + k, fileSize, mWriteBoth);
      files.add(new AlluxioURI(uniqPath + k));
    }
    for (int k = 0; k < numOfFiles; k++) {
      URIStatus info = mFileSystem.getStatus(files.get(k));
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
      is = mFileSystem.openFile(files.get(k), FileSystemTestUtils.toOpenFileOptions(mWriteBoth));
      buf = ByteBuffer.allocate((int) info.getBlockSizeBytes());
      Assert.assertTrue(is.read(buf.array()) != -1);
      is.close();
    }
    FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + numOfFiles, fileSize, mWriteBoth);
    files.add(new AlluxioURI(uniqPath + numOfFiles));

    for (int k = 1; k < numOfFiles; k++) {
      URIStatus info = mFileSystem.getStatus(files.get(k));
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
    }
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 10,
        TimeUnit.SECONDS));
    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_BLOCK_SYNC);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 10,
        TimeUnit.SECONDS));
    URIStatus info = mFileSystem.getStatus(files.get(numOfFiles));
    Assert.assertTrue(info.getInMemoryPercentage() == 100);
  }

  @Test
  public void lockBlockTest3() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    FileInStream is;
    ByteBuffer buf;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<AlluxioURI> files = new ArrayList<AlluxioURI>();
    for (int k = 0; k < numOfFiles; k++) {
      FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + k, fileSize, mWriteBoth);
      files.add(new AlluxioURI(uniqPath + k));
    }
    for (int k = 0; k < numOfFiles; k++) {
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
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 10,
        TimeUnit.SECONDS));
    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_BLOCK_SYNC);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 10,
        TimeUnit.SECONDS));
    URIStatus info = mFileSystem.getStatus(files.get(0));
    Assert.assertFalse(info.getInMemoryPercentage() == 100);
    for (int k = 1; k <= numOfFiles; k++) {
      info = mFileSystem.getStatus(files.get(k));
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
    }
  }

  @Test
  public void unlockBlockTest1() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    FileInStream is;
    ByteBuffer buf;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<AlluxioURI> files = new ArrayList<AlluxioURI>();
    for (int k = 0; k < numOfFiles; k++) {
      FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + k, fileSize, mWriteBoth);
      files.add(new AlluxioURI(uniqPath + k));
    }
    for (int k = 0; k < numOfFiles; k++) {
      URIStatus info = mFileSystem.getStatus(files.get(k));
      is = mFileSystem.openFile(files.get(k), FileSystemTestUtils.toOpenFileOptions(mWriteBoth));
      buf = ByteBuffer.allocate((int) info.getBlockSizeBytes());
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
      Assert.assertTrue(is.read(buf.array()) != -1);
      is.close();
    }
    FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + numOfFiles, fileSize, mWriteBoth);
    files.add(new AlluxioURI(uniqPath + numOfFiles));
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 10,
        TimeUnit.SECONDS));
    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_BLOCK_SYNC);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 10,
        TimeUnit.SECONDS));
    URIStatus info = mFileSystem.getStatus(files.get(0));
    Assert.assertFalse(info.getInMemoryPercentage() == 100);
    for (int k = 1; k <= numOfFiles; k++) {
      URIStatus in = mFileSystem.getStatus(files.get(k));
      Assert.assertTrue(in.getInMemoryPercentage() == 100);
    }
  }

  @Test
  public void unlockBlockTest2() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    FileInStream is;
    ByteBuffer buf;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<AlluxioURI> files = new ArrayList<AlluxioURI>();
    for (int k = 0; k < numOfFiles; k++) {
      FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + k, fileSize, mWriteBoth);
      files.add(new AlluxioURI(uniqPath + k));
    }
    for (int k = 0; k < numOfFiles; k++) {
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
    for (int k = 1; k < numOfFiles; k++) {
      URIStatus info = mFileSystem.getStatus(files.get(k));
      Assert.assertTrue(info.getInMemoryPercentage() == 100);
    }
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 10,
        TimeUnit.SECONDS));
    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_BLOCK_SYNC);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 10,
        TimeUnit.SECONDS));
    URIStatus info = mFileSystem.getStatus(files.get(numOfFiles));
    Assert.assertTrue(info.getInMemoryPercentage() == 100);
  }

  @Test
  public void unlockBlockTest3() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    FileInStream is;
    ByteBuffer buf1;
    ByteBuffer buf2;
    int numOfFiles = 5;
    int fileSize = WORKER_CAPACITY_BYTES / numOfFiles;
    List<AlluxioURI> files = new ArrayList<AlluxioURI>();
    for (int k = 0; k < numOfFiles; k++) {
      FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + k, fileSize, mWriteBoth);
      files.add(new AlluxioURI(uniqPath + k));
    }
    for (int k = 0; k < numOfFiles; k++) {
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
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 10,
        TimeUnit.SECONDS));
    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_BLOCK_SYNC);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 10,
        TimeUnit.SECONDS));
    URIStatus info = mFileSystem.getStatus(files.get(0));
    Assert.assertFalse(info.getInMemoryPercentage() == 100);
    for (int k = 1; k <= numOfFiles; k++) {
      URIStatus in = mFileSystem.getStatus(files.get(k));
      Assert.assertTrue(in.getInMemoryPercentage() == 100);
    }
  }
}
