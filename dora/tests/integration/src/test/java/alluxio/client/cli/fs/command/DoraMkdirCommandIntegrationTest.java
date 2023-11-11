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

package alluxio.client.cli.fs.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.client.cli.fs.AbstractDoraFileSystemShellTest;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;

import org.junit.Test;

import java.io.IOException;

public class DoraMkdirCommandIntegrationTest extends AbstractDoraFileSystemShellTest {
  public DoraMkdirCommandIntegrationTest() throws IOException {
    super(3);
  }

  @Override
  public void before() throws Exception {
    mLocalAlluxioClusterResource.setProperty(PropertyKey.UNDERFS_XATTR_CHANGE_ENABLED, false);
    super.before();
  }

  @Test
  public void testMkdir() throws Exception {
    AlluxioURI uri = new AlluxioURI("/testRoot/testDir");
    assertEquals(0, mFsShell.run("mkdir", uri.toString()));
    URIStatus status = mFileSystem.getStatus(uri);
    assertNotNull(status);
    assertTrue(status.isFolder());
    checkOutput("Successfully created directory " + uri);
  }

  @Test
  public void testMkdirExisting() {
    AlluxioURI uri = new AlluxioURI("/testRoot/testDir");
    assertEquals(0, mFsShell.run("mkdir", uri.toString()));
    assertEquals(-1, mFsShell.run("mkdir", uri.toString()));
    assertTrue(
        mOutput.toString().contains("ALREADY_EXISTS"));
  }

  @Test
  public void testMkdirPathWithWhiteSpaces() throws Exception {
    String[] paths = new String[] {
        "/ ",
        "/x y z",
        "/ x y z",
        "/ x y z / a b c"
    };
    for (String path : paths) {
      AlluxioURI uri = new AlluxioURI(path);
      assertEquals(0, mFsShell.run("mkdir", uri.toString()));
      URIStatus status = mFileSystem.getStatus(uri);
      assertNotNull(status);
      assertTrue(status.isFolder());
      assertTrue(mOutput.toString().contains("Successfully created directory " + uri));
    }
  }

  @Test
  public void testMkdirInvalidPath() {
    assertEquals(-1, mFsShell.run("mkdir", ""));
    assertTrue(
        mOutput.toString().contains("ALREADY_EXISTS"));
  }

  @Test
  public void testMkdirMultiPath() throws Exception {
    String path1 = "/testDir1";
    String path2 = "/testDir2";
    String path3 = "/testDir2/testDir2.1";
    assertEquals(0, mFsShell.run("mkdir", path1, path2, path3));

    URIStatus status = mFileSystem.getStatus(new AlluxioURI(path1));
    assertNotNull(status);
    assertTrue(status.isFolder());

    status = mFileSystem.getStatus(new AlluxioURI(path2));
    assertNotNull(status);
    assertTrue(status.isFolder());

    status = mFileSystem.getStatus(new AlluxioURI(path3));
    assertNotNull(status);
    assertTrue(status.isFolder());
  }

  @Test
  public void testMkdirRecursive() throws Exception {
    String path1 = "/testDir2/testDir2.1";
    String path2 = "/testDir2";
    assertEquals(0, mFsShell.run("mkdir", path1));

    URIStatus status = mFileSystem.getStatus(new AlluxioURI(path1));
    URIStatus status1 = mFileSystem.getStatus(new AlluxioURI(path2));
    assertNotNull(status);
    assertNotNull(status1);
    assertTrue(status.isFolder());
    assertTrue(status1.isFolder());
  }

  private void checkOutput(String... linePatterns) {
    String[] actualLines = mOutput.toString().split("\n");
    assertEquals("Output: ", linePatterns.length, actualLines.length);

    for (int i = 0; i < linePatterns.length; i++) {
      assertTrue("mOutput: " + mOutput.toString(), actualLines[i].matches(linePatterns[i]));
    }
    mOutput.reset();
  }
}
