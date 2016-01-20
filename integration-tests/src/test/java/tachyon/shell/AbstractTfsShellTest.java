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
import org.powermock.reflect.Whitebox;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.ClientContext;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.TachyonStorageType;
import tachyon.client.UnderStorageType;
import tachyon.client.file.FileInStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.InStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.master.LocalTachyonCluster;
import tachyon.security.LoginUser;
import tachyon.security.authentication.AuthType;
import tachyon.shell.command.CommandUtils;
import tachyon.thrift.FileInfo;
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
          Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
  protected LocalTachyonCluster mLocalTachyonCluster = null;
  protected TachyonFileSystem mTfs = null;
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
    mLocalTachyonCluster = mLocalTachyonClusterResource.get();
    mTfs = mLocalTachyonCluster.getClient();
    mFsShell = new TfsShell(new TachyonConf());
    mOutput = new ByteArrayOutputStream();
    mNewOutput = new PrintStream(mOutput);
    mOldOutput = System.out;
    System.setOut(mNewOutput);
  }

  protected void copyToLocalWithBytes(int bytes) throws IOException {
    TachyonFSTestUtils.createByteFile(mTfs, "/testFile", TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, bytes);
    mFsShell.run("copyToLocal", "/testFile", mLocalTachyonCluster.getTachyonHome() + "/testFile");
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

  protected void cleanAndLogin(String user) throws IOException {
    // clear the loginUser and re-login with new user
    synchronized (LoginUser.class) {
      Whitebox.setInternalState(LoginUser.class, "sLoginUser", (String) null);
      ClientContext.getConf().set(Constants.SECURITY_LOGIN_USERNAME, user);
      LoginUser.get(ClientContext.getConf());
    }
  }

  protected byte[] readContent(TachyonFile tFile, int length) throws IOException, TachyonException {
    InStreamOptions options =
        new InStreamOptions.Builder(new TachyonConf()).setTachyonStorageType(
            TachyonStorageType.NO_STORE).build();
    FileInStream tfis = mTfs.getInStream(tFile, options);
    byte[] read = new byte[length];
    tfis.read(read);
    return read;
  }

  protected boolean isInMemoryTest(String path) throws IOException, TachyonException {
    return (mTfs.getInfo(mTfs.open(new TachyonURI(path))).getInMemoryPercentage() == 100);
  }

  protected String getLsResultStr(TachyonURI tUri, int size, String testUser, String testGroup)
      throws IOException, TachyonException {
    FileInfo fileInfo = mTfs.getInfo(mTfs.open(tUri));
    return getLsResultStr(tUri.getPath(), fileInfo.getCreationTimeMs(), size, "In Memory",
        testUser, testGroup, fileInfo.getPermission(), fileInfo.isFolder);
  }

  protected String getLsResultStr(String path, long createTime, int size, String fileType,
      String testUser, String testGroup, int permission, boolean isDir) throws IOException,
      TachyonException {
    return String.format(Constants.COMMAND_FORMAT_LS,
        CommandUtils.formatPermission(permission, isDir), testUser, testGroup,
        FormatUtils.getSizeFromBytes(size), CommandUtils.convertMsToDate(createTime), fileType,
        path);
  }

  protected boolean fileExist(TachyonURI path) {
    try {
      return mTfs.open(path) != null;
    } catch (IOException e) {
      return false;
    } catch (TachyonException e) {
      return false;
    }
  }

  /**
   * Checks whether the given file is actually persisted by freeing it, then reading it and
   * comparing it against the expected byte array.
   *
   * @param file The file to persist
   * @param size The size of the file
   * @throws TachyonException if an unexpected tachyon exception is thrown
   * @throws IOException if a non-Tachyon exception occurs
   */
  protected void checkFilePersisted(TachyonFile file, int size) throws TachyonException,
      IOException {
    Assert.assertTrue(mTfs.getInfo(file).isIsPersisted());
    mTfs.free(file);
    FileInStream tfis = mTfs.getInStream(file);
    byte[] actual = new byte[size];
    tfis.read(actual);
    Assert.assertArrayEquals(BufferUtils.getIncreasingByteArray(size), actual);
  }
}
