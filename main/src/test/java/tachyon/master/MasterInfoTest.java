/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.master;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;

import tachyon.Constants;
import tachyon.conf.CommonConf;
import tachyon.master.MasterInfo;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.TableColumnException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TachyonException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for tachyon.MasterInfo
 */
public class MasterInfoTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private MasterInfo mMasterInfo = null;

  @Test
  public void addCheckpointTest() throws FileDoesNotExistException, SuspectedFileSizeException,
      FileAlreadyExistException, InvalidPathException, BlockInfoException, FileNotFoundException,
      TachyonException {
    int fileId = mMasterInfo.createFile("/testFile", Constants.DEFAULT_BLOCK_SIZE_BYTE);
    ClientFileInfo fileInfo = mMasterInfo.getClientFileInfo("/testFile");
    Assert.assertEquals("", fileInfo.getCheckpointPath());
    mMasterInfo.addCheckpoint(-1, fileId, 1, "/testPath");
    fileInfo = mMasterInfo.getClientFileInfo("/testFile");
    Assert.assertEquals("/testPath", fileInfo.getCheckpointPath());
    mMasterInfo.addCheckpoint(-1, fileId, 1, "/testPath");
    fileInfo = mMasterInfo.getClientFileInfo("/testFile");
    Assert.assertEquals("/testPath", fileInfo.getCheckpointPath());
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    mLocalTachyonCluster = new LocalTachyonCluster(1000);
    mLocalTachyonCluster.start();
    mMasterInfo = mLocalTachyonCluster.getMasterInfo();
  }

  @Test
  public void clientFileInfoDirectoryTest() throws InvalidPathException,
      FileDoesNotExistException, FileAlreadyExistException, TachyonException {
    Assert.assertTrue(mMasterInfo.mkdir("/testFolder"));
    ClientFileInfo fileInfo = mMasterInfo.getClientFileInfo("/testFolder");
    Assert.assertEquals("testFolder", fileInfo.getName());
    Assert.assertEquals(2, fileInfo.getId());
    Assert.assertEquals(0, fileInfo.getLength());
    Assert.assertEquals("", fileInfo.getCheckpointPath());
    Assert.assertTrue(fileInfo.isFolder());
    Assert.assertFalse(fileInfo.isNeedPin());
    Assert.assertFalse(fileInfo.isNeedCache());
    Assert.assertTrue(fileInfo.isComplete());
  }

  @Test
  public void clientFileInfoEmptyFileTest() throws InvalidPathException,
      FileDoesNotExistException, FileAlreadyExistException, BlockInfoException, TachyonException {
    int fileId = mMasterInfo.createFile("/testFile", Constants.DEFAULT_BLOCK_SIZE_BYTE);
    ClientFileInfo fileInfo = mMasterInfo.getClientFileInfo("/testFile");
    Assert.assertEquals("testFile", fileInfo.getName());
    Assert.assertEquals(fileId, fileInfo.getId());
    Assert.assertEquals(0, fileInfo.getLength());
    Assert.assertEquals("", fileInfo.getCheckpointPath());
    Assert.assertFalse(fileInfo.isFolder());
    Assert.assertFalse(fileInfo.isNeedPin());
    Assert.assertTrue(fileInfo.isNeedCache());
    Assert.assertFalse(fileInfo.isComplete());
  }

  @Test(expected = FileAlreadyExistException.class)
  public void createAlreadyExistFileTest() throws InvalidPathException, FileAlreadyExistException,
      BlockInfoException, TachyonException {
    mMasterInfo.createFile("/testFile", Constants.DEFAULT_BLOCK_SIZE_BYTE);
    mMasterInfo.mkdir("/testFile");
  }

  @Test
  public void createDirectoryTest() throws InvalidPathException, FileAlreadyExistException,
      FileDoesNotExistException, TachyonException {
    mMasterInfo.mkdir("/testFolder");
    ClientFileInfo fileInfo = mMasterInfo.getClientFileInfo("/testFolder");
    Assert.assertTrue(fileInfo.isFolder());
  }

  @Test(expected = InvalidPathException.class)
  public void createFileInvalidPathTest() throws InvalidPathException, FileAlreadyExistException,
      BlockInfoException, TachyonException {
    mMasterInfo.createFile("testFile", Constants.DEFAULT_BLOCK_SIZE_BYTE);
  }

  @Test(expected = InvalidPathException.class)
  public void createFileInvalidPathTest2() throws InvalidPathException, FileAlreadyExistException,
      BlockInfoException, TachyonException {
    mMasterInfo.createFile("/", Constants.DEFAULT_BLOCK_SIZE_BYTE);
  }

  @Test(expected = InvalidPathException.class)
  public void createFileInvalidPathTest3() throws InvalidPathException, FileAlreadyExistException,
      BlockInfoException, TachyonException {
    mMasterInfo.createFile("/testFile1", Constants.DEFAULT_BLOCK_SIZE_BYTE);
    mMasterInfo.createFile("/testFile1/testFile2", Constants.DEFAULT_BLOCK_SIZE_BYTE);
  }

  @Test
  public void createFilePerfTest() throws FileAlreadyExistException, InvalidPathException,
      FileDoesNotExistException, TachyonException {
    // long sMs = System.currentTimeMillis();
    for (int k = 0; k < 200; k ++) {
      mMasterInfo.mkdir("/testFile" + Constants.PATH_SEPARATOR + MasterInfo.COL + k
          + Constants.PATH_SEPARATOR + 0);
    }
    // System.out.println(System.currentTimeMillis() - sMs);
    // sMs = System.currentTimeMillis();
    for (int k = 0; k < 200; k ++) {
      mMasterInfo.getClientFileInfo("/testFile" + Constants.PATH_SEPARATOR + MasterInfo.COL + k
          + Constants.PATH_SEPARATOR + 0);
    }
    // System.out.println(System.currentTimeMillis() - sMs);
  }

  @Test
  public void createFileTest() throws InvalidPathException, FileAlreadyExistException,
      FileDoesNotExistException, BlockInfoException, TachyonException {
    mMasterInfo.createFile("/testFile", Constants.DEFAULT_BLOCK_SIZE_BYTE);
    Assert.assertFalse(mMasterInfo.getClientFileInfo("/testFile").isFolder());
  }

  @Test
  public void createRawTableTest() throws InvalidPathException, FileAlreadyExistException,
      TableColumnException, FileDoesNotExistException, TachyonException {
    mMasterInfo.createRawTable("/testTable", 1, (ByteBuffer) null);
    Assert.assertTrue(mMasterInfo.getClientFileInfo("/testTable").isFolder());
  }

  @Test
  public void deleteDirectoryWithDirectoriesTest() throws InvalidPathException,
      FileAlreadyExistException, TachyonException, BlockInfoException {
    Assert.assertTrue(mMasterInfo.mkdir("/testFolder"));
    Assert.assertTrue(mMasterInfo.mkdir("/testFolder/testFolder2"));
    int fileId = mMasterInfo.createFile("/testFolder/testFile", Constants.DEFAULT_BLOCK_SIZE_BYTE);
    int fileId2 =
        mMasterInfo.createFile("/testFolder/testFolder2/testFile2",
            Constants.DEFAULT_BLOCK_SIZE_BYTE);
    Assert.assertEquals(2, mMasterInfo.getFileId("/testFolder"));
    Assert.assertEquals(3, mMasterInfo.getFileId("/testFolder/testFolder2"));
    Assert.assertEquals(fileId, mMasterInfo.getFileId("/testFolder/testFile"));
    Assert.assertEquals(fileId2, mMasterInfo.getFileId("/testFolder/testFolder2/testFile2"));
    Assert.assertTrue(mMasterInfo.delete(2, true));
    Assert.assertEquals(-1, mMasterInfo.getFileId("/testFolder"));
    Assert.assertEquals(-1, mMasterInfo.getFileId("/testFolder/testFolder2"));
    Assert.assertEquals(-1, mMasterInfo.getFileId("/testFolder/testFile"));
    Assert.assertEquals(-1, mMasterInfo.getFileId("/testFolder/testFolder2/testFile2"));
  }

  @Test
  public void deleteDirectoryWithDirectoriesTest2() throws InvalidPathException,
      FileAlreadyExistException, TachyonException, BlockInfoException {
    Assert.assertTrue(mMasterInfo.mkdir("/testFolder"));
    Assert.assertTrue(mMasterInfo.mkdir("/testFolder/testFolder2"));
    int fileId = mMasterInfo.createFile("/testFolder/testFile", Constants.DEFAULT_BLOCK_SIZE_BYTE);
    int fileId2 =
        mMasterInfo.createFile("/testFolder/testFolder2/testFile2",
            Constants.DEFAULT_BLOCK_SIZE_BYTE);
    Assert.assertEquals(2, mMasterInfo.getFileId("/testFolder"));
    Assert.assertEquals(3, mMasterInfo.getFileId("/testFolder/testFolder2"));
    Assert.assertEquals(fileId, mMasterInfo.getFileId("/testFolder/testFile"));
    Assert.assertEquals(fileId2, mMasterInfo.getFileId("/testFolder/testFolder2/testFile2"));
    Assert.assertFalse(mMasterInfo.delete(2, false));
    Assert.assertEquals(2, mMasterInfo.getFileId("/testFolder"));
    Assert.assertEquals(3, mMasterInfo.getFileId("/testFolder/testFolder2"));
    Assert.assertEquals(fileId, mMasterInfo.getFileId("/testFolder/testFile"));
    Assert.assertEquals(fileId2, mMasterInfo.getFileId("/testFolder/testFolder2/testFile2"));
  }

  @Test
  public void deleteDirectoryWithFilesTest() throws InvalidPathException,
      FileAlreadyExistException, TachyonException, BlockInfoException {
    Assert.assertTrue(mMasterInfo.mkdir("/testFolder"));
    int fileId = mMasterInfo.createFile("/testFolder/testFile", Constants.DEFAULT_BLOCK_SIZE_BYTE);
    Assert.assertEquals(2, mMasterInfo.getFileId("/testFolder"));
    Assert.assertEquals(fileId, mMasterInfo.getFileId("/testFolder/testFile"));
    Assert.assertTrue(mMasterInfo.delete(2, true));
    Assert.assertEquals(-1, mMasterInfo.getFileId("/testFolder"));
    Assert.assertEquals(-1, mMasterInfo.getFileId("/testFolder/testFile"));
  }

  @Test
  public void deleteDirectoryWithFilesTest2() throws InvalidPathException,
      FileAlreadyExistException, TachyonException, BlockInfoException {
    Assert.assertTrue(mMasterInfo.mkdir("/testFolder"));
    int fileId = mMasterInfo.createFile("/testFolder/testFile", Constants.DEFAULT_BLOCK_SIZE_BYTE);
    Assert.assertEquals(2, mMasterInfo.getFileId("/testFolder"));
    Assert.assertEquals(fileId, mMasterInfo.getFileId("/testFolder/testFile"));
    Assert.assertFalse(mMasterInfo.delete(2, false));
    Assert.assertEquals(2, mMasterInfo.getFileId("/testFolder"));
    Assert.assertEquals(fileId, mMasterInfo.getFileId("/testFolder/testFile"));
  }

  @Test
  public void deleteEmptyDirectoryTest() throws InvalidPathException, FileAlreadyExistException,
      TachyonException {
    Assert.assertTrue(mMasterInfo.mkdir("/testFolder"));
    Assert.assertEquals(2, mMasterInfo.getFileId("/testFolder"));
    Assert.assertTrue(mMasterInfo.delete("/testFolder", true));
    Assert.assertEquals(-1, mMasterInfo.getFileId("/testFolder"));
  }

  @Test
  public void deleteFileTest() throws InvalidPathException, FileAlreadyExistException,
      TachyonException, BlockInfoException {
    int fileId = mMasterInfo.createFile("/testFile", Constants.DEFAULT_BLOCK_SIZE_BYTE);
    Assert.assertEquals(fileId, mMasterInfo.getFileId("/testFile"));
    Assert.assertTrue(mMasterInfo.delete(fileId, true));
    Assert.assertEquals(-1, mMasterInfo.getFileId("/testFile"));
  }

  @Test
  public void getCapacityBytesTest() {
    Assert.assertEquals(1000, mMasterInfo.getCapacityBytes());
  }

  @Test
  public void listFilesTest() throws InvalidPathException, FileDoesNotExistException,
      FileAlreadyExistException, BlockInfoException, TachyonException {
    HashSet<Integer> ids = new HashSet<Integer>();
    for (int i = 0; i < 10; i ++) {
      String dir = "/i" + i;
      mMasterInfo.mkdir(dir);
      for (int j = 0; j < 10; j ++) {
        ids.add(mMasterInfo.createFile(dir + "/j" + j, 64));
      }
    }
    HashSet<Integer> listedIds = new HashSet<Integer>(mMasterInfo.listFiles("/", true));
    Assert.assertEquals(ids, listedIds);
  }

  @Test
  public void lsTest() throws FileAlreadyExistException, InvalidPathException, TachyonException,
      BlockInfoException, FileDoesNotExistException {
    for (int i = 0; i < 10; i ++) {
      mMasterInfo.mkdir("/i" + i);
      for (int j = 0; j < 10; j ++) {
        mMasterInfo.createFile("/i" + i + "/j" + j, 64);
      }
    }

    Assert.assertEquals(1, mMasterInfo.ls("/i0/j0", false).size());
    Assert.assertEquals(1, mMasterInfo.ls("/i0/j0", true).size());
    for (int i = 0; i < 10; i ++) {
      Assert.assertEquals(11, mMasterInfo.ls("/i" + i, false).size());
      Assert.assertEquals(11, mMasterInfo.ls("/i" + i, true).size());
    }
    Assert.assertEquals(11, mMasterInfo.ls(Constants.PATH_SEPARATOR, false).size());
    Assert.assertEquals(111, mMasterInfo.ls(Constants.PATH_SEPARATOR, true).size());
  }

  @Test(expected = TableColumnException.class)
  public void negativeColumnTest() throws InvalidPathException, FileAlreadyExistException,
      TableColumnException, TachyonException {
    mMasterInfo.createRawTable("/testTable", -1, (ByteBuffer) null);
  }

  @Test(expected = FileNotFoundException.class)
  public void notFileCheckpointTest() throws FileNotFoundException, SuspectedFileSizeException,
      FileAlreadyExistException, InvalidPathException, BlockInfoException, TachyonException {
    Assert.assertTrue(mMasterInfo.mkdir("/testFile"));
    mMasterInfo.addCheckpoint(-1, mMasterInfo.getFileId("/testFile"), 0, "/testPath");
  }

  @Test(expected = FileDoesNotExistException.class)
  public void renameNonexistentTest() throws InvalidPathException, FileAlreadyExistException,
      FileDoesNotExistException, TachyonException, BlockInfoException {
    mMasterInfo.createFile("/testFile1", Constants.DEFAULT_BLOCK_SIZE_BYTE);
    mMasterInfo.rename("/testFile2", "/testFile3");
  }

  @Test
  public void renameExistingDstTest() throws InvalidPathException, FileAlreadyExistException,
      FileDoesNotExistException, TachyonException, BlockInfoException {
    mMasterInfo.createFile("/testFile1", Constants.DEFAULT_BLOCK_SIZE_BYTE);
    mMasterInfo.createFile("/testFile2", Constants.DEFAULT_BLOCK_SIZE_BYTE);
    Assert.assertFalse(mMasterInfo.rename("/testFile1", "/testFile2"));
  }

  @Test(expected = InvalidPathException.class)
  public void renameToDeeper() throws InvalidPathException, FileAlreadyExistException,
      FileDoesNotExistException, TachyonException, BlockInfoException {
    mMasterInfo.mkdir("/testDir1/testDir2");
    mMasterInfo.createFile("/testDir1/testDir2/testDir3/testFile3",
        Constants.DEFAULT_BLOCK_SIZE_BYTE);
    mMasterInfo.rename("/testDir1/testDir2", "/testDir1/testDir2/testDir3/testDir4");
  }

  @Test(expected = TableColumnException.class)
  public void tooManyColumnsTest() throws InvalidPathException, FileAlreadyExistException,
      TableColumnException, TachyonException {
    mMasterInfo.createRawTable("/testTable", CommonConf.get().MAX_COLUMNS + 1, (ByteBuffer) null);
  }
}
