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
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.IntegrationTestConstants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemCluster;
import alluxio.util.io.BufferUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;

import java.io.IOException;
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
      new LocalAlluxioClusterResource(WORKER_CAPACITY_BYTES, BLOCK_SIZE_BYTES,
          Constants.USER_FILE_BUFFER_BYTES, String.valueOf(BUFFER_BYTES),
          Constants.WORKER_DATA_SERVER, IntegrationTestConstants.NETTY_DATA_SERVER);

  protected CreateFileOptions mWriteBoth;
  protected CreateFileOptions mWriteAlluxio;
  protected CreateFileOptions mWriteLocal;
  protected CreateFileOptions mWriteAsync;
  protected CreateFileOptions mWriteUnderStore;

  protected Configuration mTestConf;
  protected FileSystem mFileSystem = null;

  @Before
  public void before() throws Exception {
    mTestConf = mLocalAlluxioClusterResource.get().getWorkerConf();
    mWriteBoth = StreamOptionUtils.getCreateFileOptionsCacheThrough();
    mWriteAlluxio = StreamOptionUtils.getCreateFileOptionsMustCache();
    mWriteUnderStore = StreamOptionUtils.getCreateFileOptionsThrough();
    mWriteLocal = StreamOptionUtils.getCreateFileOptionsWriteLocal();
    mWriteAsync = StreamOptionUtils.getCreateFileOptionsAsync();
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
  }

  /**
   * Checks that we wrote the file correctly by reading it every possible way.
   *
   * @param filePath path of the tmp file
   * @param underStorageType type of under storage write
   * @param fileLen length of the file
   * @param increasingByteArrayLen expected length of increasing bytes written in the file
   * @throws IOException if an I/O exception occurs
   */
  protected void checkWrite(AlluxioURI filePath, UnderStorageType underStorageType, int fileLen,
                            int increasingByteArrayLen) throws IOException, AlluxioException {
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
      UnderFileSystem ufs = UnderFileSystem.get(checkpointPath, mTestConf);

      InputStream is = ufs.open(checkpointPath);
      byte[] res = new byte[(int) status.getLength()];
      String underFSClass = UnderFileSystemCluster.getUnderFSClass();
      if ("alluxio.underfs.hdfs.LocalMiniDFSCluster".equals(underFSClass)
          && 0 == res.length) {
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
    List<CreateFileOptions> ret = new ArrayList<CreateFileOptions>(3);
    ret.add(mWriteBoth);
    ret.add(mWriteAlluxio);
    ret.add(mWriteUnderStore);
    return ret;
  }
}
