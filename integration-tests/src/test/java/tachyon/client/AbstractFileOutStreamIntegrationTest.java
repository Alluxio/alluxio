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

package tachyon.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;

import tachyon.Constants;
import tachyon.IntegrationTestConstants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.file.FileInStream;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.FileSystem;
import tachyon.client.file.URIStatus;
import tachyon.client.file.options.CreateFileOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.underfs.UnderFileSystem;
import tachyon.underfs.UnderFileSystemCluster;
import tachyon.util.io.BufferUtils;

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
  public LocalTachyonClusterResource mLocalTachyonClusterResource =
      new LocalTachyonClusterResource(WORKER_CAPACITY_BYTES, QUOTA_UNIT_BYTES, BLOCK_SIZE_BYTES,
          Constants.USER_FILE_BUFFER_BYTES, String.valueOf(BUFFER_BYTES),
          Constants.WORKER_DATA_SERVER, IntegrationTestConstants.NETTY_DATA_SERVER);

  protected CreateFileOptions mWriteBoth;
  protected CreateFileOptions mWriteTachyon;
  protected CreateFileOptions mWriteLocal;
  protected CreateFileOptions mWriteAsync;
  protected CreateFileOptions mWriteUnderStore;

  protected TachyonConf mTestConf;
  protected FileSystem mTfs = null;

  @Before
  public void before() throws Exception {
    mTestConf = mLocalTachyonClusterResource.get().getWorkerTachyonConf();
    mWriteBoth = StreamOptionUtils.getCreateFileOptionsCacheThrough(mTestConf);
    mWriteTachyon = StreamOptionUtils.getCreateFileOptionsMustCache(mTestConf);
    mWriteUnderStore = StreamOptionUtils.getCreateFileOptionsThrough(mTestConf);
    mWriteLocal = StreamOptionUtils.getCreateFileOptionsWriteLocal(mTestConf);
    mWriteAsync = StreamOptionUtils.getCreateFileOptionsAsync(mTestConf);
    mTfs = mLocalTachyonClusterResource.get().getClient();
  }

  /**
   * Checks that we wrote the file correctly by reading it every possible way
   *
   * @param filePath path of the tmp file
   * @param underStorageType type of understorage write
   * @param fileLen length of the file
   * @param increasingByteArrayLen expected length of increasing bytes written in the file
   * @throws IOException
   */
  protected void checkWrite(TachyonURI filePath, UnderStorageType underStorageType, int fileLen,
                          int increasingByteArrayLen) throws IOException, TachyonException {
    for (CreateFileOptions op : getOptionSet()) {
      URIStatus status = mTfs.getStatus(filePath);
      Assert.assertEquals(fileLen, status.getLength());
      FileInStream is = mTfs.openFile(filePath, TachyonFSTestUtils.toOpenFileOptions(op));
      byte[] res = new byte[(int) status.getLength()];
      Assert.assertEquals((int) status.getLength(), is.read(res));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(increasingByteArrayLen, res));
      is.close();
    }

    if (underStorageType.isSyncPersist() || underStorageType.isAsyncPersist()) {
      URIStatus status = mTfs.getStatus(filePath);
      String checkpointPath = status.getUfsPath();
      UnderFileSystem ufs = UnderFileSystem.get(checkpointPath, mTestConf);

      InputStream is = ufs.open(checkpointPath);
      byte[] res = new byte[(int) status.getLength()];
      if (UnderFileSystemCluster.readEOFReturnsNegative() && 0 == res.length) {
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
    ret.add(mWriteTachyon);
    ret.add(mWriteUnderStore);
    return ret;
  }
}
