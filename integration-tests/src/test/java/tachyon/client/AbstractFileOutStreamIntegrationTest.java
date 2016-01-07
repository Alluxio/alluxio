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
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.InStreamOptions;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.thrift.FileInfo;
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

  protected OutStreamOptions mWriteBoth;
  protected OutStreamOptions mWriteTachyon;
  protected OutStreamOptions mWriteLocal;
  protected OutStreamOptions mWriteAsync;
  protected TachyonConf mTestConf;

  protected OutStreamOptions mWriteUnderStore;
  protected TachyonFileSystem mTfs = null;

  @Before
  public void before() throws Exception {
    mTestConf = mLocalTachyonClusterResource.get().getWorkerTachyonConf();
    mWriteBoth = StreamOptionUtils.getOutStreamOptionsWriteBoth(mTestConf);
    mWriteTachyon = StreamOptionUtils.getOutStreamOptionsWriteTachyon(mTestConf);
    mWriteUnderStore = StreamOptionUtils.getOutStreamOptionsWriteUnderStore(mTestConf);
    mWriteLocal = StreamOptionUtils.getOutStreamOptionsWriteLocal(mTestConf);
    mWriteAsync = StreamOptionUtils.getOutStreamOptionsWriteAsync(mTestConf);
    mTfs = mLocalTachyonClusterResource.get().getClient();
  }

  /**
   * Checks that we wrote the file correctly by reading it every possible way
   *
   * @param filePath path of the tmp file
   * @param underStorageType type of understorage write
   * @param fileLen length of the file
   * @param increasingByteArrayLen expected length of increasing bytes written in the file
   * @throws IOException if an I/O exception occurs
   */
  protected void checkWrite(TachyonURI filePath, UnderStorageType underStorageType, int fileLen,
      int increasingByteArrayLen) throws IOException, TachyonException {
    for (OutStreamOptions op : getOptionSet()) {
      TachyonFile file = mTfs.open(filePath);
      FileInfo info = mTfs.getInfo(file);
      Assert.assertEquals(fileLen, info.getLength());
      InStreamOptions op2 =
          new InStreamOptions.Builder(mTestConf).setTachyonStorageType(
              op.getTachyonStorageType()).build();
      FileInStream is = mTfs.getInStream(file, op2);
      byte[] res = new byte[(int) info.getLength()];
      Assert.assertEquals((int) info.getLength(), is.read(res));
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(increasingByteArrayLen, res));
      is.close();
    }

    if (underStorageType.isSyncPersist() || underStorageType.isAsyncPersist()) {
      TachyonFile file = mTfs.open(filePath);
      FileInfo info = mTfs.getInfo(file);
      String checkpointPath = info.getUfsPath();
      UnderFileSystem ufs = UnderFileSystem.get(checkpointPath, mTestConf);

      InputStream is = ufs.open(checkpointPath);
      byte[] res = new byte[(int) info.getLength()];
      if (UnderFileSystemCluster.readEOFReturnsNegative() && 0 == res.length) {
        // Returns -1 for zero-sized byte array to indicate no more bytes available here.
        Assert.assertEquals(-1, is.read(res));
      } else {
        Assert.assertEquals((int) info.getLength(), is.read(res));
      }
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(increasingByteArrayLen, res));
      is.close();
    }
  }

  protected List<OutStreamOptions> getOptionSet() {
    List<OutStreamOptions> ret = new ArrayList<OutStreamOptions>(3);
    ret.add(mWriteBoth);
    ret.add(mWriteTachyon);
    ret.add(mWriteUnderStore);
    return ret;
  }
}
