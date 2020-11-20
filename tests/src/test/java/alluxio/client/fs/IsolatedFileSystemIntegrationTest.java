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

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.LocalAlluxioCluster;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Integration tests on Alluxio client (do not reuse the {@link LocalAlluxioCluster}).
 */
public class IsolatedFileSystemIntegrationTest extends BaseIntegrationTest {
  private static final int WORKER_CAPACITY_BYTES = 200 * Constants.MB;
  private static final int WORKER_UNRESERVED_BYTES = WORKER_CAPACITY_BYTES / 10 * 9;
  private static final int USER_QUOTA_UNIT_BYTES = 1000;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.WORKER_RAMDISK_SIZE, WORKER_CAPACITY_BYTES)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, 100 * Constants.MB)
          .setProperty(PropertyKey.USER_FILE_RESERVED_BYTES, USER_QUOTA_UNIT_BYTES)
          .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, USER_QUOTA_UNIT_BYTES)
          .build();
  private FileSystem mFileSystem = null;
  private CreateFilePOptions mWriteBoth;

  @Before
  public final void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    mWriteBoth = CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH)
        .setRecursive(true).build();
  }

  @Test
  public void lockBlockTest1() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    int numOfFiles = 5;
    int fileSize = WORKER_UNRESERVED_BYTES / numOfFiles;
    List<AlluxioURI> files = new ArrayList<>();
    for (int k = 0; k < numOfFiles; k++) {
      FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + k, fileSize, mWriteBoth);
      files.add(new AlluxioURI(uniqPath + k));
    }
    for (int k = 0; k < numOfFiles; k++) {
      Assert.assertEquals(100, mFileSystem.getStatus(files.get(k)).getInAlluxioPercentage());
    }
    FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + numOfFiles, fileSize, mWriteBoth);
    files.add(new AlluxioURI(uniqPath + numOfFiles));

    Assert.assertEquals(numOfFiles, numCached(files));
    Assert.assertEquals(100, mFileSystem.getStatus(files.get(numOfFiles)).getInAlluxioPercentage());
    Assert.assertNotEquals(100, mFileSystem.getStatus(files.get(0)).getInAlluxioPercentage());
  }

  @Test
  public void lockBlockTest2() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    FileInStream is;
    ByteBuffer buf;
    int numOfFiles = 5;
    int fileSize = WORKER_UNRESERVED_BYTES / numOfFiles;
    List<AlluxioURI> files = new ArrayList<>();
    for (int k = 0; k < numOfFiles; k++) {
      FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + k, fileSize, mWriteBoth);
      files.add(new AlluxioURI(uniqPath + k));
    }
    for (int k = 0; k < numOfFiles; k++) {
      URIStatus info = mFileSystem.getStatus(files.get(k));
      Assert.assertTrue(info.getInAlluxioPercentage() == 100);
      is = mFileSystem.openFile(files.get(k), FileSystemTestUtils.toOpenFileOptions(mWriteBoth));
      buf = ByteBuffer.allocate((int) info.getBlockSizeBytes());
      Assert.assertTrue(is.read(buf.array()) != -1);
      is.close();
    }
    FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + numOfFiles, fileSize, mWriteBoth);
    files.add(new AlluxioURI(uniqPath + numOfFiles));

    Assert.assertEquals(numOfFiles, numCached(files));
    Assert.assertEquals(100, mFileSystem.getStatus(files.get(numOfFiles)).getInAlluxioPercentage());
    Assert.assertNotEquals(100, mFileSystem.getStatus(files.get(0)).getInAlluxioPercentage());
  }

  @Test
  public void lockBlockTest3() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    FileInStream is;
    ByteBuffer buf;
    int numOfFiles = 5;
    int fileSize = WORKER_UNRESERVED_BYTES / numOfFiles;
    List<AlluxioURI> files = new ArrayList<>();
    for (int k = 0; k < numOfFiles; k++) {
      FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + k, fileSize, mWriteBoth);
      files.add(new AlluxioURI(uniqPath + k));
    }
    for (int k = 0; k < numOfFiles; k++) {
      URIStatus info = mFileSystem.getStatus(files.get(k));
      Assert.assertTrue(info.getInAlluxioPercentage() == 100);
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
    Assert.assertEquals(numOfFiles, numCached(files));
    Assert.assertEquals(100, mFileSystem.getStatus(files.get(numOfFiles)).getInAlluxioPercentage());
    Assert.assertNotEquals(100, mFileSystem.getStatus(files.get(0)).getInAlluxioPercentage());
  }

  @Test
  public void unlockBlockTest1() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    FileInStream is;
    ByteBuffer buf;
    int numOfFiles = 5;
    int fileSize = WORKER_UNRESERVED_BYTES / numOfFiles;
    List<AlluxioURI> files = new ArrayList<>();
    for (int k = 0; k < numOfFiles; k++) {
      FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + k, fileSize, mWriteBoth);
      files.add(new AlluxioURI(uniqPath + k));
    }
    for (int k = 0; k < numOfFiles; k++) {
      URIStatus info = mFileSystem.getStatus(files.get(k));
      is = mFileSystem.openFile(files.get(k), FileSystemTestUtils.toOpenFileOptions(mWriteBoth));
      buf = ByteBuffer.allocate((int) info.getBlockSizeBytes());
      Assert.assertTrue(info.getInAlluxioPercentage() == 100);
      Assert.assertTrue(is.read(buf.array()) != -1);
      is.close();
    }
    FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + numOfFiles, fileSize, mWriteBoth);
    files.add(new AlluxioURI(uniqPath + numOfFiles));
    Assert.assertEquals(numOfFiles, numCached(files));
    Assert.assertEquals(100, mFileSystem.getStatus(files.get(numOfFiles)).getInAlluxioPercentage());
    Assert.assertNotEquals(100, mFileSystem.getStatus(files.get(0)).getInAlluxioPercentage());
  }

  @Test
  public void unlockBlockTest2() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    FileInStream is;
    ByteBuffer buf;
    int numOfFiles = 5;
    int fileSize = WORKER_UNRESERVED_BYTES / numOfFiles;
    List<AlluxioURI> files = new ArrayList<>();
    for (int k = 0; k < numOfFiles; k++) {
      FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + k, fileSize, mWriteBoth);
      files.add(new AlluxioURI(uniqPath + k));
    }
    for (int k = 0; k < numOfFiles; k++) {
      URIStatus info = mFileSystem.getStatus(files.get(k));
      Assert.assertTrue(info.getInAlluxioPercentage() == 100);
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
    Assert.assertEquals(numOfFiles, numCached(files));
    Assert.assertEquals(100, mFileSystem.getStatus(files.get(numOfFiles)).getInAlluxioPercentage());
    Assert.assertNotEquals(100, mFileSystem.getStatus(files.get(0)).getInAlluxioPercentage());
  }

  @Test
  public void unlockBlockTest3() throws Exception {
    String uniqPath = PathUtils.uniqPath();
    FileInStream is;
    ByteBuffer buf1;
    ByteBuffer buf2;
    int numOfFiles = 5;
    int fileSize = WORKER_UNRESERVED_BYTES / numOfFiles;
    List<AlluxioURI> files = new ArrayList<>();
    for (int k = 0; k < numOfFiles; k++) {
      FileSystemTestUtils.createByteFile(mFileSystem, uniqPath + k, fileSize, mWriteBoth);
      files.add(new AlluxioURI(uniqPath + k));
    }
    for (int k = 0; k < numOfFiles; k++) {
      URIStatus info = mFileSystem.getStatus(files.get(k));
      Assert.assertTrue(info.getInAlluxioPercentage() == 100);
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
    Assert.assertEquals(numOfFiles, numCached(files));
    Assert.assertEquals(100, mFileSystem.getStatus(files.get(numOfFiles)).getInAlluxioPercentage());
    Assert.assertNotEquals(100, mFileSystem.getStatus(files.get(0)).getInAlluxioPercentage());
  }

  /**
   * Returns number of fully cached files from the list of uris.
   *
   * @param fileUris list of file uris
   * @return number of fully cached files among the input list of uris
   */
  private long numCached(List<AlluxioURI> fileUris) {
    return fileUris.stream().map((uri) -> {
      try {
        return mFileSystem.getStatus(uri);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).filter((status) -> status.getInAlluxioPercentage() == 100).count();
  }
}
