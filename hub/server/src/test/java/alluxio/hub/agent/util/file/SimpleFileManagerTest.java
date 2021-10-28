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

package alluxio.hub.agent.util.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;

import alluxio.security.authorization.Mode;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Set;

public class SimpleFileManagerTest {

  @Rule
  public TemporaryFolder mTemp = new TemporaryFolder();

  private String mUser;
  private String mGroup;
  private String mRootDir;
  private SimpleFileManager mFileManager;

  @Before
  public void before() {
    mRootDir = mTemp.getRoot().getAbsolutePath();
    mUser = System.getProperty("user.name");
    mGroup = PrestoCatalogUtils.getPrimaryGroup(mUser);
    mFileManager = new SimpleFileManager(mRootDir, mUser, mGroup);
  }

  @Test
  public void addOneFile() throws Exception {
    assertTrue(mFileManager.addFile("test", "644", "test".getBytes()));
    List<String> files = mFileManager.listFile();
    assertEquals(1, files.size());
    assertEquals("test", files.get(0));
    Path file = Paths.get(mFileManager.getNextFilePath("test"));
    PosixFileAttributes attrs = Files.getFileAttributeView(file, PosixFileAttributeView.class)
        .readAttributes();
    assertEquals(mUser, attrs.owner().getName());
    assertEquals(mGroup, attrs.group().getName());
    verifyPerms(attrs.permissions(), "644");
  }

  @Test
  public void addMultipleFileSameName() throws Exception {
    assertTrue(mFileManager.addFile("test", "644", "test".getBytes()));
    List<String> files = mFileManager.listFile();
    assertEquals(1, files.size());
    assertEquals("test", files.get(0));
    Path file = Paths.get(mFileManager.getNextFilePath("test"));
    PosixFileAttributes attrs = Files.getFileAttributeView(file, PosixFileAttributeView.class)
        .readAttributes();
    assertEquals(mUser, attrs.owner().getName());
    assertEquals(mGroup, attrs.group().getName());
    verifyPerms(attrs.permissions(), "644");

    // Create a new file with the same name, but different content and permissions
    assertTrue(mFileManager.addFile("test", "777", "test 2".getBytes()));
    files = mFileManager.listFile();
    assertEquals(1, files.size());
    assertEquals("test", files.get(0));
    file = Paths.get(mFileManager.getNextFilePath("test"));
    attrs = Files.getFileAttributeView(file, PosixFileAttributeView.class)
        .readAttributes();
    assertEquals(mUser, attrs.owner().getName());
    assertEquals(mGroup, attrs.group().getName());
    verifyPerms(attrs.permissions(), "777");
  }

  @Test
  public void testInvalidFilename() {
    assertFalse(mFileManager.addFile("../test", "644", "test".getBytes()));
    assertThrows(InvalidPathException.class, () -> {
      mFileManager.getNextFilePath("../test");
    });
    List<String> files = mFileManager.listFile();
    assertEquals(0, files.size());
  }

  @Test
  public void testRemoveFile() {
    assertTrue(mFileManager.addFile("test", "644", "test".getBytes()));
    assertEquals(1, mFileManager.listFile().size());
    assertTrue(mFileManager.removeFile("test"));
    assertEquals(0, mFileManager.listFile().size());
  }

  @Test
  public void testRemoveNonExistentFile() {
    assertTrue(mFileManager.removeFile("test"));
    assertEquals(0, mFileManager.listFile().size());
  }

  @Test
  public void testFailedCreate() {
    try (MockedStatic<Files> mock = Mockito.mockStatic(Files.class)) {
      mock.when(() -> Files.createFile(any(), any()))
          .thenThrow(new IOException("Failed to set permissions!"));
      assertFalse(mFileManager.addFile("test", "0644", "test".getBytes()));
    }
  }

  @Test
  public void testEmptyFilename() {
    assertFalse(mFileManager.addFile("", "0644", "test".getBytes()));
  }

  private static void verifyPerms(Set<PosixFilePermission> perms, String actual) {
    short perm = Short.parseShort(actual, 8);
    Mode mode = new Mode(perm);
    Set<PosixFilePermission> actualPerms = PosixFilePermissions.fromString(mode.toString());
    assertTrue(perms.containsAll(actualPerms));
    assertEquals(actualPerms.size(), perms.size());
  }
}
