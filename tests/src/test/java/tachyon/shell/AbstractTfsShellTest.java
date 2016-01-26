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

package tachyon.shell;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.ClientContext;
import tachyon.client.ReadType;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.WriteType;
import tachyon.client.file.FileInStream;
import tachyon.client.file.FileSystem;
import tachyon.client.file.URIStatus;
import tachyon.client.file.options.OpenFileOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.master.LocalTachyonCluster;
import tachyon.security.LoginUserTestUtils;
import tachyon.security.authentication.AuthType;
import tachyon.shell.command.CommandUtils;
import tachyon.util.FormatUtils;
import tachyon.util.io.BufferUtils;
import tachyon.util.io.PathUtils;

/**
 * The base class for all the TfsShellTest classes.
 */
public abstract class AbstractTfsShellTest {
  protected static final int SIZE_BYTES = Constants.MB * 10;
  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource =
      new LocalTachyonClusterResource(SIZE_BYTES, 1000, Constants.MB,
          Constants.MASTER_TTLCHECKER_INTERVAL_MS, String.valueOf(Integer.MAX_VALUE),
          Constants.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true",
          Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
  protected LocalTachyonCluster mLocalTachyonCluster = null;
  protected FileSystem mTfs = null;
  protected TfsShell mFsShell = null;
  protected ByteArrayOutputStream mOutput = null;
  protected PrintStream mNewOutput = null;
  protected PrintStream mOldOutput = null;

  @Rule
  public ExpectedException mException = ExpectedException.none();

  @After
  public final void after() throws Exception {
    mFsShell.close();
    System.setOut(mOldOutput);
  }

  @Before
  public final void before() throws Exception {
    cleanLoginUser();
    mLocalTachyonCluster = mLocalTachyonClusterResource.get();
    mTfs = mLocalTachyonCluster.getClient();
    mFsShell = new TfsShell(new TachyonConf());
    mOutput = new ByteArrayOutputStream();
    mNewOutput = new PrintStream(mOutput);
    mOldOutput = System.out;
    System.setOut(mNewOutput);
  }

  protected void copyToLocalWithBytes(int bytes) throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", WriteType.MUST_CACHE, bytes);
    mFsShell.run("copyToLocal", "/testFile",
        mLocalTachyonCluster.getTachyonHome() + "/testFile");
    Assert.assertEquals(getCommandOutput(new String[] {"copyToLocal", "/testFile",
        mLocalTachyonCluster.getTachyonHome() + "/testFile"}), mOutput.toString());
    fileReadTest("/testFile", 10);
  }

  protected void fileReadTest(String fileName, int size) throws IOException {
    File testFile = new File(PathUtils.concatPath(mLocalTachyonCluster.getTachyonHome(), fileName));
    FileInputStream fis = new FileInputStream(testFile);
    byte[] read = new byte[size];
    fis.read(read);
    fis.close();
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(size, read));
  }

  protected File generateFileContent(String path, byte[] toWrite) throws IOException,
      FileNotFoundException {
    File testFile = new File(mLocalTachyonCluster.getTachyonHome() + path);
    testFile.createNewFile();
    FileOutputStream fos = new FileOutputStream(testFile);
    fos.write(toWrite);
    fos.close();
    return testFile;
  }

  protected String getCommandOutput(String[] command) {
    String cmd = command[0];
    if (command.length == 2) {
      if (cmd.equals("ls")) {
        // Not sure how to handle this one.
        return null;
      } else if (cmd.equals("mkdir")) {
        return "Successfully created directory " + command[1] + "\n";
      } else if (cmd.equals("rm") || cmd.equals("rmr")) {
        return command[1] + " has been removed" + "\n";
      } else if (cmd.equals("touch")) {
        return command[1] + " has been created" + "\n";
      }
    } else if (command.length == 3) {
      if (cmd.equals("mv")) {
        return "Renamed " + command[1] + " to " + command[2] + "\n";
      } else if (cmd.equals("copyFromLocal")) {
        return "Copied " + command[1] + " to " + command[2] + "\n";
      } else if (cmd.equals("copyToLocal")) {
        return "Copied " + command[1] + " to " + command[2] + "\n";
      }
    } else if (command.length > 3) {
      if (cmd.equals("location")) {
        StringBuilder ret = new StringBuilder();
        ret.append(command[1] + " with file id " + command[2] + " is on nodes: \n");
        for (int i = 3; i < command.length; i ++) {
          ret.append(command[i] + "\n");
        }
        return ret.toString();
      } else if (cmd.equals("fileInfo")) {
        StringBuilder ret = new StringBuilder();
        ret.append(command[1] + " with file id " + command[2] + " has the following blocks: \n");
        for (int i = 3; i < command.length; i ++) {
          ret.append(command[i] + "\n");
        }
        return ret.toString();
      }
    }
    return null;
  }

  protected void cleanLoginUser() {
    LoginUserTestUtils.resetLoginUser();
  }

  protected void cleanAndLogin(String user) throws IOException {
    LoginUserTestUtils.updateLoginUser(ClientContext.getConf(), user);
  }

  protected byte[] readContent(TachyonURI uri, int length) throws IOException, TachyonException {
    FileInStream tfis =
        mTfs.openFile(uri, OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE));
    byte[] read = new byte[length];
    tfis.read(read);
    return read;
  }

  protected boolean isInMemoryTest(String path) throws IOException, TachyonException {
    return (mTfs.getStatus(new TachyonURI(path)).getInMemoryPercentage() == 100);
  }

  protected String getLsResultStr(TachyonURI tUri, int size, String testUser, String testGroup)
      throws IOException, TachyonException {
    URIStatus status = mTfs.getStatus(tUri);
    return getLsResultStr(tUri.getPath(), status.getCreationTimeMs(), size, "In Memory",
        testUser, testGroup, status.getPermission(), status.isFolder());
  }

  protected String getLsResultStr(String path, long createTime, int size, String fileType,
      String testUser, String testGroup, int permission, boolean isDir) throws IOException,
      TachyonException {
    return String.format(Constants.COMMAND_FORMAT_LS,
        FormatUtils.formatPermission((short) permission, isDir), testUser, testGroup,
        FormatUtils.getSizeFromBytes(size), CommandUtils.convertMsToDate(createTime), fileType,
        path);
  }

  protected boolean fileExist(TachyonURI path) {
    try {
      return mTfs.exists(path);
    } catch (IOException e) {
      return false;
    } catch (TachyonException e) {
      return false;
    }
  }

  /**
   * Checks whether the given file is actually persisted by freeing it, then
   * reading it and comparing it against the expected byte array.
   *
   * @param uri The uri to persist
   * @param size The size of the file
   * @throws TachyonException if an unexpected tachyon exception is thrown
   * @throws IOException if a non-Tachyon exception occurs
   */
  protected void checkFilePersisted(TachyonURI uri, int size) throws TachyonException, IOException {
    Assert.assertTrue(mTfs.getStatus(uri).isPersisted());
    mTfs.free(uri);
    FileInStream tfis = mTfs.openFile(uri);
    byte[] actual = new byte[size];
    tfis.read(actual);
    Assert.assertArrayEquals(BufferUtils.getIncreasingByteArray(size), actual);
  }
}
