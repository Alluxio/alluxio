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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import tachyon.Constants;
import tachyon.IntegrationTestConstants;
import tachyon.TachyonURI;
import tachyon.TestUtils;
import tachyon.conf.TachyonConf;
import tachyon.master.LocalTachyonCluster;

/**
 * Integration tests for <code>tachyon.client.RemoteBlockInStream</code>.
 */
@RunWith(Parameterized.class)
public class RemoteBlockInStreamIntegrationTest {
  private static final int MIN_LEN = 0;
  private static final int MAX_LEN = 255;
  private static final int DELTA = 33;

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTfs = null;
  private String mDataServerClass;
  private String mRemoteReaderClass;
  private TachyonConf mMasterTachyonConf;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    // creates a new instance of RemoteBlockInStreamTest for each network type
    List<Object[]> list = new ArrayList<Object[]>();
    list.add(new Object[] { new String[] { IntegrationTestConstants.NETTY_DATA_SERVER,
        IntegrationTestConstants.TCP_BLOCK_READER } });
    list.add(new Object[] { new String[] { IntegrationTestConstants.NETTY_DATA_SERVER,
        IntegrationTestConstants.NETTY_BLOCK_READER } });
    list.add(new Object[] { new String[] { IntegrationTestConstants.NIO_DATA_SERVER,
        IntegrationTestConstants.TCP_BLOCK_READER } });
    list.add(new Object[] { new String[] { IntegrationTestConstants.NIO_DATA_SERVER,
        IntegrationTestConstants.NETTY_BLOCK_READER } });
    return list;
  }

  public RemoteBlockInStreamIntegrationTest(String[] classes) {
    mDataServerClass = classes[0];
    mRemoteReaderClass = classes[1];
  }

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
  }

  @Before
  public final void before() throws IOException {
    mLocalTachyonCluster = new LocalTachyonCluster(10000, 1000, Constants.GB);
    System.setProperty(Constants.WORKER_DATA_SERVER, mDataServerClass);
    System.setProperty(Constants.USER_REMOTE_BLOCK_READER, mRemoteReaderClass);
    mLocalTachyonCluster.start();
    mLocalTachyonCluster.getWorkerTachyonConf().set(Constants.USER_REMOTE_READ_BUFFER_SIZE_BYTE,
        "100");
    mMasterTachyonConf = mLocalTachyonCluster.getMasterTachyonConf();
    mTfs = mLocalTachyonCluster.getClient();
  }

  /**
   * Test <code>void read()</code>. Read from underfs.
   */
  @Test
  public void readTest1() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      WriteType op = WriteType.THROUGH;
      int fileId =
          TachyonFSTestUtils.createByteFile(mTfs, uniqPath + "/file_" + k + "_" + op, op, k);

      TachyonFile file = mTfs.getFile(fileId);
      InStream is = file.getInStream(ReadType.NO_CACHE);
      if (k == 0) {
        Assert.assertTrue(is instanceof EmptyBlockInStream);
      } else {
        Assert.assertTrue(is instanceof RemoteBlockInStream);
      }
      byte[] ret = new byte[k];
      int value = is.read();
      int cnt = 0;
      while (value != -1) {
        Assert.assertTrue(value >= 0);
        Assert.assertTrue(value < 256);
        ret[cnt ++] = (byte) value;
        value = is.read();
      }
      Assert.assertEquals(cnt, k);
      Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, ret));
      is.close();
      if (k == 0) {
        Assert.assertTrue(file.isInMemory());
      } else {
        Assert.assertFalse(file.isInMemory());
      }

      is = file.getInStream(ReadType.CACHE);
      if (k == 0) {
        Assert.assertTrue(is instanceof EmptyBlockInStream);
      } else {
        Assert.assertTrue(is instanceof RemoteBlockInStream);
      }
      ret = new byte[k];
      value = is.read();
      cnt = 0;
      while (value != -1) {
        Assert.assertTrue(value >= 0);
        Assert.assertTrue(value < 256);
        ret[cnt ++] = (byte) value;
        value = is.read();
      }
      Assert.assertEquals(cnt, k);
      Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, ret));
      is.close();
      Assert.assertTrue(file.isInMemory());

      is = file.getInStream(ReadType.CACHE);
      if (k == 0) {
        Assert.assertTrue(is instanceof EmptyBlockInStream);
      } else {
        Assert.assertTrue(is instanceof LocalBlockInStream);
      }
      ret = new byte[k];
      value = is.read();
      cnt = 0;
      while (value != -1) {
        Assert.assertTrue(value >= 0);
        Assert.assertTrue(value < 256);
        ret[cnt ++] = (byte) value;
        value = is.read();
      }
      Assert.assertEquals(cnt, k);
      Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, ret));
      is.close();
      Assert.assertTrue(file.isInMemory());
    }
  }

  /**
   * Test <code>void read(byte[] b)</code>. Read from underfs.
   */
  @Test
  public void readTest2() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      WriteType op = WriteType.THROUGH;
      int fileId =
          TachyonFSTestUtils.createByteFile(mTfs, uniqPath + "/file_" + k + "_" + op, op, k);

      TachyonFile file = mTfs.getFile(fileId);
      InStream is = file.getInStream(ReadType.NO_CACHE);
      if (k == 0) {
        Assert.assertTrue(is instanceof EmptyBlockInStream);
      } else {
        Assert.assertTrue(is instanceof RemoteBlockInStream);
      }
      byte[] ret = new byte[k];
      Assert.assertEquals(k, is.read(ret));
      Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, ret));
      is.close();
      if (k == 0) {
        Assert.assertTrue(file.isInMemory());
      } else {
        Assert.assertFalse(file.isInMemory());
      }

      is = file.getInStream(ReadType.CACHE);
      if (k == 0) {
        Assert.assertTrue(is instanceof EmptyBlockInStream);
      } else {
        Assert.assertTrue(is instanceof RemoteBlockInStream);
      }
      ret = new byte[k];
      Assert.assertEquals(k, is.read(ret));
      Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, ret));
      is.close();
      Assert.assertTrue(file.isInMemory());

      is = file.getInStream(ReadType.CACHE);
      if (k == 0) {
        Assert.assertTrue(is instanceof EmptyBlockInStream);
      } else {
        Assert.assertTrue(is instanceof LocalBlockInStream);
      }
      ret = new byte[k];
      Assert.assertEquals(k, is.read(ret));
      Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, ret));
      is.close();
      Assert.assertTrue(file.isInMemory());
    }
  }

  /**
   * Test <code>void read(byte[] b, int off, int len)</code>. Read from underfs.
   */
  @Test
  public void readTest3() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      WriteType op = WriteType.THROUGH;
      int fileId =
          TachyonFSTestUtils.createByteFile(mTfs, uniqPath + "/file_" + k + "_" + op, op, k);

      TachyonFile file = mTfs.getFile(fileId);
      InStream is = file.getInStream(ReadType.NO_CACHE);
      if (k == 0) {
        Assert.assertTrue(is instanceof EmptyBlockInStream);
      } else {
        Assert.assertTrue(is instanceof RemoteBlockInStream);
      }
      byte[] ret = new byte[k / 2];
      Assert.assertEquals(k / 2, is.read(ret, 0, k / 2));
      Assert.assertTrue(TestUtils.equalIncreasingByteArray(k / 2, ret));
      is.close();
      if (k == 0) {
        Assert.assertTrue(file.isInMemory());
      } else {
        Assert.assertFalse(file.isInMemory());
      }

      is = file.getInStream(ReadType.CACHE);
      if (k == 0) {
        Assert.assertTrue(is instanceof EmptyBlockInStream);
      } else {
        Assert.assertTrue(is instanceof RemoteBlockInStream);
      }
      ret = new byte[k];
      Assert.assertEquals(k, is.read(ret, 0, k));
      Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, ret));
      is.close();
      Assert.assertTrue(file.isInMemory());

      is = file.getInStream(ReadType.CACHE);
      if (k == 0) {
        Assert.assertTrue(is instanceof EmptyBlockInStream);
      } else {
        Assert.assertTrue(is instanceof LocalBlockInStream);
      }
      ret = new byte[k];
      Assert.assertEquals(k, is.read(ret));
      Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, ret));
      is.close();
      Assert.assertTrue(file.isInMemory());
    }
  }

  /**
   * Test <code>void read()</code>. Read from remote data server.
   */
  @Test
  public void readTest4() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      WriteType op = WriteType.MUST_CACHE;
      int fileId =
          TachyonFSTestUtils.createByteFile(mTfs, uniqPath + "/file_" + k + "_" + op, op, k);

      TachyonFile file = mTfs.getFile(fileId);
      RemoteBlockInStream is =
          new RemoteBlockInStream(file, ReadType.NO_CACHE, 0, mMasterTachyonConf);
      Assert.assertTrue(is instanceof RemoteBlockInStream);
      byte[] ret = new byte[k];
      int value = is.read();
      int cnt = 0;
      while (value != -1) {
        Assert.assertTrue(value >= 0);
        Assert.assertTrue(value < 256);
        ret[cnt ++] = (byte) value;
        value = is.read();
      }
      Assert.assertEquals(cnt, k);
      Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, ret));
      is.close();
      Assert.assertTrue(file.isInMemory());
    }
  }

  /**
   * Test <code>void read(byte[] b)</code>. Read from remote data server.
   */
  @Test
  public void readTest5() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      WriteType op = WriteType.MUST_CACHE;
      int fileId =
          TachyonFSTestUtils.createByteFile(mTfs, uniqPath + "/file_" + k + "_" + op, op, k);

      TachyonFile file = mTfs.getFile(fileId);
      InStream is = new RemoteBlockInStream(file, ReadType.NO_CACHE, 0, mMasterTachyonConf);
      Assert.assertTrue(is instanceof RemoteBlockInStream);
      byte[] ret = new byte[k];
      int start = 0;
      while (start < k) {
        int read = is.read(ret);
        Assert.assertTrue(TestUtils.equalIncreasingByteArray(start, read, ret));
        start += read;
      }
      is.close();
      Assert.assertTrue(file.isInMemory());
    }
  }

  /**
   * Test <code>void read(byte[] b, int off, int len)</code>. Read from remote data server.
   */
  @Test
  public void readTest6() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      WriteType op = WriteType.MUST_CACHE;
      int fileId =
          TachyonFSTestUtils.createByteFile(mTfs, uniqPath + "/file_" + k + "_" + op, op, k);

      TachyonFile file = mTfs.getFile(fileId);
      InStream is = new RemoteBlockInStream(file, ReadType.NO_CACHE, 0, mMasterTachyonConf);
      Assert.assertTrue(is instanceof RemoteBlockInStream);
      byte[] ret = new byte[k / 2];
      int start = 0;
      while (start < k / 2) {
        int read = is.read(ret, 0, (k / 2) - start);
        Assert.assertTrue(TestUtils.equalIncreasingByteArray(start, read, ret));
        start += read;
      }
      is.close();
      Assert.assertTrue(file.isInMemory());
    }
  }

  /**
   * Test <code>void read(byte[] b)</code>. Read from underfs.
   */
  @Test
  public void readTest7() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      WriteType op = WriteType.THROUGH;
      int fileId =
          TachyonFSTestUtils.createByteFile(mTfs, uniqPath + "/file_" + k + "_" + op, op, k);

      TachyonFile file = mTfs.getFile(fileId);
      InStream is = file.getInStream(ReadType.NO_CACHE);
      if (k == 0) {
        Assert.assertTrue(is instanceof EmptyBlockInStream);
      } else {
        Assert.assertTrue(is instanceof RemoteBlockInStream);
      }
      byte[] ret = new byte[k];
      Assert.assertEquals(k, is.read(ret));
      Assert.assertTrue(TestUtils.equalIncreasingByteArray(k, ret));
      Assert.assertEquals(-1, is.read(ret));
      is.close();
      Assert.assertFalse(file.isInMemory());
    }
  }

  /**
   * Test <code>void seek(long pos)</code>. Validate the expected exception for seeking a negative
   * position.
   *
   * @throws IOException
   */
  @Test
  public void seekExceptionTest1() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      WriteType op = WriteType.THROUGH;
      int fileId =
          TachyonFSTestUtils.createByteFile(mTfs, uniqPath + "/file_" + k + "_" + op, op, k);

      TachyonFile file = mTfs.getFile(fileId);
      InStream is = file.getInStream(ReadType.NO_CACHE);
      if (k == 0) {
        Assert.assertTrue(is instanceof EmptyBlockInStream);
      } else {
        Assert.assertTrue(is instanceof RemoteBlockInStream);
      }

      try {
        is.seek(-1);
      } catch (IOException e) {
        // This is expected
        continue;
      }
      is.close();
      throw new IOException("Except seek IOException");
    }
  }

  /**
   * Test <code>void seek(long pos)</code>. Validate the expected exception for seeking a position
   * that is past block size.
   *
   * @throws IOException
   */
  @Test
  public void seekExceptionTest2() throws IOException {
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Seek position is past block size");
    String uniqPath = TestUtils.uniqPath();
    for (int k = MIN_LEN; k <= MAX_LEN; k += DELTA) {
      WriteType op = WriteType.THROUGH;
      int fileId =
          TachyonFSTestUtils.createByteFile(mTfs, uniqPath + "/file_" + k + "_" + op, op, k);

      TachyonFile file = mTfs.getFile(fileId);
      InStream is = file.getInStream(ReadType.NO_CACHE);
      if (k == 0) {
        Assert.assertTrue(is instanceof EmptyBlockInStream);
      } else {
        Assert.assertTrue(is instanceof RemoteBlockInStream);
      }

      is.seek(k + 1);
      is.close();
    }
  }

  /**
   * Test <code>void seek(long pos)</code>.
   *
   * @throws IOException
   */
  @Test
  public void seekTest() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      WriteType op = WriteType.THROUGH;
      int fileId =
          TachyonFSTestUtils.createByteFile(mTfs, uniqPath + "/file_" + k + "_" + op, op, k);

      TachyonFile file = mTfs.getFile(fileId);
      InStream is = file.getInStream(ReadType.NO_CACHE);
      if (k == 0) {
        Assert.assertTrue(is instanceof EmptyBlockInStream);
      } else {
        Assert.assertTrue(is instanceof RemoteBlockInStream);
      }

      Assert.assertEquals(0, is.read());
      is.seek(k / 3);
      Assert.assertEquals(k / 3, is.read());
      is.seek(k / 2);
      Assert.assertEquals(k / 2, is.read());
      is.seek(k / 4);
      Assert.assertEquals(k / 4, is.read());
      is.close();
    }
  }

  /**
   * Test <code>long skip(long len)</code>.
   */
  @Test
  public void skipTest() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    for (int k = MIN_LEN + DELTA; k <= MAX_LEN; k += DELTA) {
      WriteType op = WriteType.THROUGH;
      int fileId =
          TachyonFSTestUtils.createByteFile(mTfs, uniqPath + "/file_" + k + "_" + op, op, k);

      TachyonFile file = mTfs.getFile(fileId);
      InStream is = file.getInStream(ReadType.CACHE);
      Assert.assertTrue(is instanceof RemoteBlockInStream);
      Assert.assertEquals(k / 2, is.skip(k / 2));
      Assert.assertEquals(k / 2, is.read());
      is.close();
      Assert.assertFalse(file.isInMemory());

      if (k >= 3) {
        is = file.getInStream(ReadType.CACHE);
        Assert.assertTrue(is instanceof RemoteBlockInStream);
        int t = k / 3;
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals(t, is.read());
        Assert.assertEquals(t, is.skip(t));
        Assert.assertEquals(2 * t + 1, is.read());
        is.close();
        Assert.assertFalse(file.isInMemory());
      }
    }
  }

  /**
   * Tests that reading a file the whole way through with the CACHE ReadType will recache it
   */
  @Test
  public void completeFileReadTriggersRecache() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    int len = 2;
    int fileId = TachyonFSTestUtils.createByteFile(mTfs, uniqPath, WriteType.THROUGH, len);
    TachyonFile file = mTfs.getFile(fileId);
    InStream is = file.getInStream(ReadType.CACHE);
    Assert.assertTrue(is instanceof RemoteBlockInStream);
    for (int i = 0; i < len; ++ i) {
      Assert.assertEquals(i, is.read());
    }
    is.close();
    Assert.assertTrue(file.isInMemory());
    is = file.getInStream(ReadType.NO_CACHE);
    Assert.assertTrue(is instanceof LocalBlockInStream);
  }

  /**
   * Tests that not reading a file the whole way through then closing it will cause it to not
   * recache
   */
  @Test
  public void incompleteFileReadCancelsRecache() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    int fileId = TachyonFSTestUtils.createByteFile(mTfs, uniqPath, WriteType.THROUGH, 2);
    TachyonFile file = mTfs.getFile(fileId);
    InStream is = new RemoteBlockInStream(file, ReadType.CACHE, 0, mMasterTachyonConf);
    Assert.assertEquals(0, is.read());
    is.close();
    Assert.assertFalse(file.isInMemory());
    is = file.getInStream(ReadType.NO_CACHE);
    Assert.assertTrue(is instanceof RemoteBlockInStream);
    is.close();
  }

  /**
   * Tests that reading a file consisting of more than one block from the underfs works
   */
  @Test
  public void readMultiBlockFile() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    int blockSizeByte = 10;
    int numBlocks = 10;
    int fileId = mTfs.createFile(new TachyonURI(uniqPath), blockSizeByte);
    TachyonFile file = mTfs.getFile(fileId);
    OutStream os = file.getOutStream(WriteType.THROUGH);
    for (int i = 0; i < numBlocks; i ++) {
      for (int j = 0; j < blockSizeByte; j ++) {
        os.write((byte) (i * blockSizeByte + j));
      }
    }
    os.close();

    for (int i = 0; i < numBlocks; i ++) {
      InStream is = new RemoteBlockInStream(file, ReadType.CACHE, i, mMasterTachyonConf);
      for (int j = 0; j < blockSizeByte; j ++) {
        Assert.assertEquals((byte) (i * blockSizeByte + j), is.read());
      }
      is.close();
    }
    Assert.assertTrue(file.isInMemory());
  }

  /**
   * Tests that seeking around a file cached locally works.
   */
  @Test
  public void seekAroundLocalBlock() throws IOException {
    String uniqPath = TestUtils.uniqPath();
    // The number of bytes per remote block read should be set to 100 in the before function
    int fileId = TachyonFSTestUtils.createByteFile(mTfs, uniqPath, WriteType.MUST_CACHE, 200);
    TachyonFile file = mTfs.getFile(fileId);
    InStream is = new RemoteBlockInStream(file, ReadType.NO_CACHE, 0, mMasterTachyonConf);
    Assert.assertEquals(0, is.read());
    is.seek(199);
    Assert.assertEquals(199, is.read());
    is.seek(99);
    Assert.assertEquals(99, is.read());
    is.close();
  }
}
