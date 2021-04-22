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

package alluxio.client.cli.fs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.cli.fs.FileSystemShell;
import alluxio.cli.job.JobShell;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.grpc.WritePType;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.master.job.JobMaster;
import alluxio.security.group.GroupMappingService;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.annotation.Nullable;

/**
 * The base class for all the {@link FileSystemShell} test classes.
 */
public abstract class AbstractFileSystemShellTest extends AbstractShellIntegrationTest {
  public static LocalAlluxioCluster sLocalAlluxioCluster;
  public static FileSystem sFileSystem;
  public static FileSystemShell sFsShell;
  protected static JobMaster sJobMaster;
  protected static LocalAlluxioJobCluster sLocalAlluxioJobCluster;
  protected static JobShell sJobShell;

  /*
   * The user and group mappings for testing are:
   *    alice -> alice,staff
   *    bob   -> bob,staff
   */
  protected static final TestUser TEST_USER_1 =
      new TestUser("alice", "alice,staff");
  protected static final TestUser TEST_USER_2 =
      new TestUser("bob", "bob,staff");

  /**
   * A simple structure to represent a user and its groups.
   */
  protected static final class TestUser {
    private String mUser;
    private String mGroup;

    TestUser(String user, String group) {
      mUser = user;
      mGroup = group;
    }

    public String getUser() {
      return mUser;
    }

    public String getGroup() {
      return mGroup;
    }
  }

  /**
   * Test class implements {@link GroupMappingService} providing user-to-groups mapping.
   */
  public static class FakeUserGroupsMapping implements GroupMappingService {
    private HashMap<String, String> mUserGroups = new HashMap<>();

    /**
     * Constructor of {@link FakeUserGroupsMapping} to put the user and groups in user-to-groups
     * HashMap.
     */
    public FakeUserGroupsMapping() {
      mUserGroups.put(TEST_USER_1.getUser(), TEST_USER_1.getGroup());
      mUserGroups.put(TEST_USER_2.getUser(), TEST_USER_2.getGroup());
    }

    @Override
    public List<String> getGroups(String user) throws IOException {
      if (mUserGroups.containsKey(user)) {
        return Lists.newArrayList(mUserGroups.get(user).split(","));
      }
      return new ArrayList<>();
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    sLocalAlluxioCluster = sLocalAlluxioClusterResource.get();
    sLocalAlluxioJobCluster = new LocalAlluxioJobCluster();
    sLocalAlluxioJobCluster.start();
    sFileSystem = sLocalAlluxioCluster.getClient();
    sJobMaster = sLocalAlluxioJobCluster.getMaster().getJobMaster();
    sJobShell = new alluxio.cli.job.JobShell(ServerConfiguration.global());
    sFsShell = new FileSystemShell(ServerConfiguration.global());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (sFsShell != null) {
      sFsShell.close();
    }
    if (sLocalAlluxioJobCluster != null) {
      sLocalAlluxioJobCluster.stop();
    }
    if (sJobShell != null) {
      sJobShell.close();
    }
  }

  /**
   * Tests the "copyToLocal" {@link FileSystemShell} command.
   *
   * @param bytes file size
   */
  protected void copyToLocalWithBytes(int bytes) throws Exception {
    FileSystemTestUtils.createByteFile(sFileSystem, "/testFile", WritePType.MUST_CACHE,
        bytes);
    sFsShell.run("copyToLocal", "/testFile",
        sLocalAlluxioCluster.getAlluxioHome() + "/testFile");
    assertEquals(getCommandOutput(new String[] {"copyToLocal", "/testFile",
        sLocalAlluxioCluster.getAlluxioHome() + "/testFile"}), mOutput.toString());
    fileReadTest("/testFile", 10);
  }

  /**
   * Reads the local file copied from Alluxio and checks all the data.
   *
   * @param fileName file name
   * @param size file size
   */
  protected void fileReadTest(String fileName, int size) throws IOException {
    File testFile = new File(PathUtils.concatPath(sLocalAlluxioCluster.getAlluxioHome(), fileName));
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
    File testFile = new File(sLocalAlluxioCluster.getAlluxioHome() + path);
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
   * Reads content from the file that the uri points to.
   *
   * @param uri the path of the file to read
   * @param length the length of content to read
   * @return the content that has been read
   */
  protected byte[] readContent(AlluxioURI uri, int length) throws IOException, AlluxioException {
    try (FileInStream tfis = sFileSystem.openFile(uri,
        OpenFilePOptions.newBuilder().setReadType(ReadPType.NO_CACHE).build())) {
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
    return (sFileSystem.getStatus(new AlluxioURI(path)).getInMemoryPercentage() == 100);
  }

  /**
   * @param path a file path
   * @return whether the file exists
   */
  protected boolean fileExists(AlluxioURI path) {
    try {
      return sFileSystem.exists(path);
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
    assertTrue(sFileSystem.getStatus(uri).isPersisted());
    CommonUtils.waitFor("file to be completely freed", () -> {
      try {
        // Call free inside the loop in case a worker reports blocks after the call to free.
        sFileSystem.free(uri);
        return sFileSystem.getStatus(uri).getInAlluxioPercentage() == 0;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, WaitForOptions.defaults().setTimeoutMs(10000));
    try (FileInStream tfis = sFileSystem.openFile(uri)) {
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
    int ret = sFsShell.run(command);
    assertEquals(expectedReturnValue, ret);
    assertTrue(mOutput.toString().contains(expectedOutput));
  }
}
