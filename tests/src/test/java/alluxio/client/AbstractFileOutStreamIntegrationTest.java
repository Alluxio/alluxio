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

package alluxio.client;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.IntegrationTestConstants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemCluster;
import alluxio.underfs.hdfs.LocalMiniDFSCluster;
import alluxio.underfs.swift.SwiftUnderStorageCluster;
import alluxio.util.io.BufferUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract classes for all integration tests of {@link FileOutStream}.
 */
public abstract class AbstractFileOutStreamIntegrationTest {
  protected static final int MIN_LEN = 0;
  protected static final int MAX_LEN = 255;
  protected static final int DELTA = 32;
  protected static final int BUFFER_BYTES = 100;
  protected static final long WORKER_CAPACITY_BYTES = Constants.GB;
  protected static final int QUOTA_UNIT_BYTES = 128;
  protected static final int BLOCK_SIZE_BYTES = 128;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, String.valueOf(BUFFER_BYTES))
          .setProperty(PropertyKey.WORKER_DATA_SERVER_CLASS,
              IntegrationTestConstants.NETTY_DATA_SERVER)
          .build();

  protected FileSystem mFileSystem = null;

  @Before
  public void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
  }

  /**
   * Checks that we wrote the file correctly by reading it every possible way.
   *
   * @param filePath path of the tmp file
   * @param underStorageType type of under storage write
   * @param fileLen length of the file
   * @param increasingByteArrayLen expected length of increasing bytes written in the file
   */
  protected void checkWrite(AlluxioURI filePath, UnderStorageType underStorageType, int fileLen,
      int increasingByteArrayLen) throws Exception {
    for (CreateFileOptions op : getOptionSet()) {
      URIStatus status = mFileSystem.getStatus(filePath);
      Assert.assertEquals(fileLen, status.getLength());
      FileInStream is = mFileSystem.openFile(filePath, FileSystemTestUtils.toOpenFileOptions(op));
      byte[] res = new byte[(int) status.getLength()];
      Assert.assertEquals((int) status.getLength(), is.read(res));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(increasingByteArrayLen, res));
      is.close();
    }

    if (underStorageType.isSyncPersist() || underStorageType.isAsyncPersist()) {
      URIStatus status = mFileSystem.getStatus(filePath);
      String checkpointPath = status.getUfsPath();
      UnderFileSystem ufs = UnderFileSystem.get(checkpointPath);

      InputStream is = ufs.open(checkpointPath);
      byte[] res = new byte[(int) status.getLength()];
      String underFSClass = UnderFileSystemCluster.getUnderFSClass();
      if ((LocalMiniDFSCluster.class.getName().equals(underFSClass)
          || SwiftUnderStorageCluster.class.getName().equals(underFSClass)) && 0 == res.length) {
        // Returns -1 for zero-sized byte array to indicate no more bytes available here.
        Assert.assertEquals(-1, is.read(res));
      } else {
        Assert.assertEquals((int) status.getLength(), is.read(res));
      }
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(increasingByteArrayLen, res));
      is.close();
    }
  }

  protected List<CreateFileOptions> getOptionSet() {
    List<CreateFileOptions> ret = new ArrayList<>(3);
    ret.add(CreateFileOptions.defaults().setWriteType(WriteType.CACHE_THROUGH));
    ret.add(CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE));
    ret.add(CreateFileOptions.defaults().setWriteType(WriteType.THROUGH));
    return ret;
  }
}
