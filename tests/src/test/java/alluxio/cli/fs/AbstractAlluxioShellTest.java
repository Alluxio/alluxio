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

package alluxio.cli.fs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.BaseIntegrationTest;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.SystemOutRule;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.master.LocalAlluxioCluster;
import alluxio.security.LoginUserTestUtils;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import javax.annotation.Nullable;

/**
 * The base class for all the {@link FileSystemShell} test classes.
 */
public abstract class AbstractAlluxioShellTest extends BaseIntegrationTest {
  protected static final int SIZE_BYTES = Constants.MB * 16;
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.WORKER_MEMORY_SIZE, SIZE_BYTES)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, SIZE_BYTES)
          .setProperty(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS, Integer.MAX_VALUE).build();
  protected LocalAlluxioCluster mLocalAlluxioCluster = null;
  protected FileSystem mFileSystem = null;
  protected FileSystemShell mFsShell = null;
  protected ByteArrayOutputStream mOutput = new ByteArrayOutputStream();

  @Rule
  public ExpectedException mException = ExpectedException.none();

  @Rule
  public SystemOutRule mOutRule = new SystemOutRule(mOutput);

  @After
  public final void after() throws Exception {
    mFsShell.close();
  }

  @Before
  public final void before() throws Exception {
    clearLoginUser();
    mLocalAlluxioCluster = mLocalAlluxioClusterResource.get();
    mFileSystem = mLocalAlluxioCluster.getClient();
    mFsShell = new FileSystemShell();
  }

  /**
   * Tests the "copyToLocal" {@link FileSystemShell} command.
   *
   * @param bytes file size
   */
  protected void copyToLocalWithBytes(int bytes) throws Exception {
    FileSystemTestUtils.createByteFile(mFileSystem, "/testFile", WriteType.MUST_CACHE, bytes);
    mFsShell.run("copyToLocal", "/testFile",
        mLocalAlluxioCluster.getAlluxioHome() + "/testFile");
    assertEquals(getCommandOutput(new String[] {"copyToLocal", "/testFile",
        mLocalAlluxioCluster.getAlluxioHome() + "/testFile"}), mOutput.toString());
    fileReadTest("/testFile", 10);
  }

  /**
   * Reads the local file copied from Alluxio and checks all the data.
   *
   * @param fileName file name
   * @param size file size
   */
  protected void fileReadTest(String fileName, int size) throws IOException {
    File testFile = new File(PathUtils.concatPath(mLocalAlluxioCluster.getAlluxioHome(), fileName));
    FileInputStream fis = new FileInputStream(testFile);
    byte[] read = new byte[size];
    fis.read(read);
    fis.close();
    assertTrue(BufferUtils.equalIncreasingByteArray(size, read));
  }

  /**
   * Creates file by given path and writes content to file.
   *
   * @param path the file path
   * @param toWrite the file content
   * @return the created file instance
   * @throws FileNotFoundException if file not found
   */
  protected File generateFileContent(String path, byte[] toWrite) throws IOException,
      FileNotFoundException {
    File testFile = new File(mLocalAlluxioCluster.getAlluxioHome() + path);
    testFile.createNewFile();
    FileOutputStream fos = new FileOutputStream(testFile);
    fos.write(toWrite);
    fos.close();
    return testFile;
  }

  /**
   * Creates file by given the temporary path and writes content to file.
   *
   * @param path the file path
   * @param toWrite the file content
   * @return the created file instance
   * @throws FileNotFoundException if file not found
   */
  protected File generateRelativeFileContent(String path, byte[] toWrite) throws IOException,
      FileNotFoundException {
    File testFile = new File(path);
    testFile.createNewFile();
    FileOutputStream fos = new FileOutputStream(testFile);
    fos.write(toWrite);
    fos.close();
    return testFile;
  }

  /**
   * Get an output string displayed in shell according to the command.
   *
   * @param command a shell command
   * @return the output string
   */
  @Nullable
  protected String getCommandOutput(String[] command) {
    String cmd = command[0];
    if (command.length == 2) {
      switch (cmd) {
        case "ls":
          // Not sure how to handle this one.
          return null;
        case "mkdir":
          return "Successfully created directory " + command[1] + "\n";
        case "rm":
        case "rmr":
          return command[1] + " has been removed" + "\n";
        case "touch":
          return command[1] + " has been created" + "\n";
        default:
          return null;
      }
    } else if (command.length == 3) {
      switch (cmd) {
        case "mv":
          return "Renamed " + command[1] + " to " + command[2] + "\n";
        case "copyFromLocal":
          return "Copied " + "file://" + command[1] + " to " + command[2] + "\n";
        case "copyToLocal":
          return "Copied " + command[1] + " to " + "file://" + command[2] + "\n";
        case "cp":
          return "Copied " + command[1] + " to " + command[2] + "\n";
        default:
          return null;
      }
    } else if (command.length > 3) {
      if (cmd.equals("location")) {
        StringBuilder ret = new StringBuilder();
        ret.append(command[1] + " with file id " + command[2] + " is on nodes: \n");
        for (int i = 3; i < command.length; i++) {
          ret.append(command[i] + "\n");
        }
        return ret.toString();
      } else if (cmd.equals("fileInfo")) {
        StringBuilder ret = new StringBuilder();
        ret.append(command[1] + " with file id " + command[2] + " has the following blocks: \n");
        for (int i = 3; i < command.length; i++) {
          ret.append(command[i] + "\n");
        }
        return ret.toString();
      }
    }
    return null;
  }

  /**
   * Resets the singleton {@link alluxio.security.LoginUser} to null.
   */
  protected void clearLoginUser() {
    LoginUserTestUtils.resetLoginUser();
  }

  /**
   * Clears the {@link alluxio.security.LoginUser} and logs in with new user.
   *
   * @param user the new user
   */
  protected void clearAndLogin(String user) throws IOException {
    LoginUserTestUtils.resetLoginUser(user);
  }

  /**
   * Reads content from the file that the uri points to.
   *
   * @param uri the path of the file to read
   * @param length the length of content to read
   * @return the content that has been read
   */
  protected byte[] readContent(AlluxioURI uri, int length) throws IOException, AlluxioException {
    try (FileInStream tfis = mFileSystem
        .openFile(uri, OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE))) {
      byte[] read = new byte[length];
      tfis.read(read);
      return read;
    }
  }

  /**
   * @param path a file path
   * @return whether the file is in memory
   */
  protected boolean isInMemoryTest(String path) throws IOException, AlluxioException {
    return (mFileSystem.getStatus(new AlluxioURI(path)).getInMemoryPercentage() == 100);
  }

  /**
   * @param path a file path
   * @return whether the file exists
   */
  protected boolean fileExists(AlluxioURI path) {
    try {
      return mFileSystem.exists(path);
    } catch (IOException e) {
      return false;
    } catch (AlluxioException e) {
      return false;
    }
  }

  /**
   * Checks whether the given file is actually persisted by freeing it, then
   * reading it and comparing it against the expected byte array.
   *
   * @param uri The uri to persist
   * @param size The size of the file
   */
  protected void checkFilePersisted(AlluxioURI uri, int size) throws Exception {
    assertTrue(mFileSystem.getStatus(uri).isPersisted());
    mFileSystem.free(uri);
    try (FileInStream tfis = mFileSystem.openFile(uri)) {
      byte[] actual = new byte[size];
      tfis.read(actual);
      assertArrayEquals(BufferUtils.getIncreasingByteArray(size), actual);
    }
  }

  /**
   * Verifies the return value and output of executing command meet expectations.
   *
   * @param expectedReturnValue the expected return value
   * @param expectedOutput the expected output string
   * @param command command to execute
   */
  protected void verifyCommandReturnValueAndOutput(int expectedReturnValue, String expectedOutput,
      String... command) {
    int ret = mFsShell.run(command);
    assertEquals(expectedReturnValue, ret);
    assertTrue(mOutput.toString().contains(expectedOutput));
  }
}
