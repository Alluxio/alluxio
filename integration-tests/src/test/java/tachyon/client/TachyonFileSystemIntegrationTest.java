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

import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.MkdirOptions;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.InvalidPathException;
import tachyon.exception.TachyonException;
import tachyon.exception.TachyonExceptionType;
import tachyon.master.LocalTachyonCluster;
import tachyon.thrift.FileInfo;
import tachyon.util.io.PathUtils;

/**
 * Integration tests on TachyonClient (Reuse the LocalTachyonCluster).
 */
public class TachyonFileSystemIntegrationTest {
  private static final int WORKER_CAPACITY_BYTES = 20000;
  private static final int USER_QUOTA_UNIT_BYTES = 1000;
  private static LocalTachyonCluster sLocalTachyonCluster = null;
  private static TachyonFileSystem sTfs = null;
  private static OutStreamOptions sWriteBoth;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public final void before() throws IOException, TException {
  }

  @AfterClass
  public static final void afterClass() throws Exception {
    sLocalTachyonCluster.stop();
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    sLocalTachyonCluster =
        new LocalTachyonCluster(WORKER_CAPACITY_BYTES, USER_QUOTA_UNIT_BYTES, Constants.GB);
    TachyonConf testConf = sLocalTachyonCluster.newTestConf();
    testConf.set(Constants.USER_FILE_BUFFER_BYTES, Integer.toString(
        USER_QUOTA_UNIT_BYTES));
    sLocalTachyonCluster.start(testConf);
    sTfs = sLocalTachyonCluster.getClient();

    sWriteBoth =
        new OutStreamOptions.Builder(sLocalTachyonCluster.getMasterTachyonConf())
            .setTachyonStorageType(TachyonStorageType.STORE)
            .setUnderStorageType(UnderStorageType.SYNC_PERSIST).build();
  }

  @Test
  public void getRootTest() throws IOException, TachyonException {
    Assert.assertEquals(0, sTfs.getInfo(sTfs.open(new TachyonURI("/"))).getFileId());
  }

  @Test
  public void createFileTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    for (int k = 1; k < 5; k ++) {
      TachyonURI uri = new TachyonURI(uniqPath + k);
      sTfs.getOutStream(uri, sWriteBoth).close();
      Assert.assertNotNull(sTfs.getInfo(sTfs.open(uri)));
    }
  }

  @Test
  public void createFileWithFileAlreadyExistsExceptionTest() throws IOException, TachyonException {
    TachyonURI uri = new TachyonURI(PathUtils.uniqPath());
    sTfs.getOutStream(uri, sWriteBoth).close();
    Assert.assertNotNull(sTfs.getInfo(sTfs.open(uri)));
    try {
      sTfs.getOutStream(uri, sWriteBoth);
    } catch (TachyonException e) {
      Assert.assertEquals(e.getType(), TachyonExceptionType.FILE_ALREADY_EXISTS);
    }
  }

  @Test
  public void createFileWithInvalidPathExceptionTest() throws IOException, TachyonException {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("Path root/testFile1 is invalid.");
    sTfs.getOutStream(new TachyonURI("root/testFile1"), sWriteBoth);
  }

  @Test
  public void deleteFileTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();

    for (int k = 0; k < 5; k ++) {
      TachyonURI fileURI = new TachyonURI(uniqPath + k);
      TachyonFile f = TachyonFSTestUtils.createByteFile(sTfs, fileURI.getPath(), k, sWriteBoth);
      Assert.assertTrue(sTfs.getInfo(f).getInMemoryPercentage() == 100);
      Assert.assertNotNull(sTfs.getInfo(f));
    }

    for (int k = 0; k < 5; k ++) {
      TachyonURI fileURI = new TachyonURI(uniqPath + k);
      TachyonFile f = sTfs.open(fileURI);
      sTfs.delete(f);
      Assert.assertNull(sTfs.openIfExists(fileURI));
      mThrown.expect(FileDoesNotExistException.class);
      sTfs.getInfo(f);
    }
  }

  @Test
  public void getFileStatusTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    int writeBytes = USER_QUOTA_UNIT_BYTES * 2;
    TachyonURI uri = new TachyonURI(uniqPath);
    TachyonFile f = TachyonFSTestUtils.createByteFile(sTfs, uri.getPath(), writeBytes, sWriteBoth);
    Assert.assertTrue(sTfs.getInfo(f).getInMemoryPercentage() == 100);
    FileInfo fileInfo = sTfs.getInfo(f);
    Assert.assertNotNull(fileInfo);
    Assert.assertTrue(fileInfo.getPath().equals(uniqPath));
  }

  @Test
  public void mkdirTest() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    MkdirOptions options = new MkdirOptions.Builder(new TachyonConf()).setRecursive(true).build();
    for (int k = 0; k < 10; k ++) {
      Assert.assertTrue(sTfs.mkdir(new TachyonURI(uniqPath + k), options));
      try {
        Assert.assertFalse(sTfs.mkdir(new TachyonURI(uniqPath + k), options));
        Assert.assertTrue("mkdir should throw FileAlreadyExistsException", false);
      } catch (FileAlreadyExistsException faee) {
        Assert.assertEquals(faee.getMessage(),
            ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(uniqPath + k));
      }
    }
  }

  @Test
  public void renameFileTest1() throws IOException, TachyonException {
    String uniqPath = PathUtils.uniqPath();
    TachyonURI path1 = new TachyonURI(uniqPath + 1);
    sTfs.getOutStream(path1, sWriteBoth).close();
    for (int k = 1; k < 10; k ++) {
      TachyonURI fileA = new TachyonURI(uniqPath + k);
      TachyonURI fileB = new TachyonURI(uniqPath + (k + 1));
      TachyonFile existingFile = sTfs.open(fileA);
      long oldFileId = existingFile.getFileId();
      Assert.assertNotNull(existingFile);
      Assert.assertTrue(sTfs.rename(existingFile, fileB));
      TachyonFile renamedFile = sTfs.open(fileB);
      Assert.assertNotNull(renamedFile);
      Assert.assertEquals(oldFileId, renamedFile.getFileId());
    }
  }

  @Test
  public void renameFileTest2() throws IOException, TachyonException {
    TachyonURI uniqUri = new TachyonURI(PathUtils.uniqPath());
    sTfs.getOutStream(uniqUri, sWriteBoth).close();
    TachyonFile f = sTfs.open(uniqUri);
    long oldFileId = f.getFileId();
    Assert.assertTrue(sTfs.rename(f, uniqUri));
    Assert.assertEquals(oldFileId, sTfs.open(uniqUri).getFileId());
  }
}
