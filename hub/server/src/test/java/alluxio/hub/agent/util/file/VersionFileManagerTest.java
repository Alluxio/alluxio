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
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class VersionFileManagerTest {
  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();

  private VersionFileManager mManager;
  private String mRootDir;

  @Before
  public void before() throws Exception {
    String user = System.getProperty("user.name");
    String group = PrestoCatalogUtils.getPrimaryGroup(user);
    mRootDir = mTempFolder.getRoot().getCanonicalPath();
    mManager = new VersionFileManager(mRootDir, user, group);
  }

  // ignore because permission is not granted to get user and group information on github containers
  @Test
  public void testAddFile() {
    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < 4; j++) {
        assertTrue(mManager.addFile("filename" + i, "777", new String("test" + i).getBytes()));
      }
    }
    assertEquals(100, mManager.listFile().size());
  }

  // ignore because permission is not granted to get user and group information on github containers
  @Test
  public void testRemoveFile() {
    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < 4; j++) {
        assertTrue(mManager.addFile("filename" + i, "777", new String("test" + i).getBytes()));
      }
    }

    for (int i = 50; i < 100; i++) {
      assertTrue(mManager.removeFile("filename" + i));
    }
    assertEquals(50, mManager.listFile().size());
  }

  @Test
  public void testOpsWithBadFile() throws Exception {
    assertTrue(mManager.addFile("filename", "777", ("test" + 1).getBytes()));
    assertTrue(mManager.addFile("filename", "777", ("test" + 2).getBytes()));
    assertTrue(mManager.addFile("filename", "777", ("test" + 3).getBytes()));
    // Add a file which doesn't properly parse
    Path f1 = Paths.get(mRootDir, "testfile");
    Path f2 = Paths.get(mRootDir, VersionFileManager.FILE_PREFIX
            + String.join(VersionFileManager.FILE_VERSION_PREFIX, "a", "b", "c"));
    Path f3 = Paths.get(mRootDir,
        VersionFileManager.FILE_PREFIX + VersionFileManager.FILE_VERSION_PREFIX + "a");
    Files.createFile(f1);
    Files.createFile(f2);
    Files.createFile(f3);
    assertTrue(mManager.addFile("filename2", "777", "test".getBytes()));
    assertEquals(2, mManager.listFile().size());
    assertTrue(mManager.removeFile("filename"));
  }
}
