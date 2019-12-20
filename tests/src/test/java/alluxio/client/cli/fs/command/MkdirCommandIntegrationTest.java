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

import alluxio.AlluxioURI;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for mkdir command.
 */
public final class MkdirCommandIntegrationTest extends AbstractFileSystemShellTest {
  @Test
  public void mkdir() throws IOException, AlluxioException {
    String qualifiedPath =
        "alluxio://" + sLocalAlluxioCluster.getHostname() + ":" + sLocalAlluxioCluster
            .getMasterRpcPort() + "/root/testFile1";
    sFsShell.run("mkdir", qualifiedPath);
    URIStatus status = sFileSystem.getStatus(new AlluxioURI("/root/testFile1"));
    Assert.assertNotNull(status);
    Assert
        .assertEquals(getCommandOutput(new String[] {"mkdir", qualifiedPath}), mOutput.toString());
    Assert.assertTrue(status.isFolder());
  }

  @Test
  public void mkdirComplexPath() throws IOException, AlluxioException {
    sFsShell.run("mkdir", "/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File");
    URIStatus status =
        sFileSystem.getStatus(new AlluxioURI("/Complex!@#$%^&*()-_=+[]{};\"'<>,.?/File"));
    Assert.assertNotNull(status);
    Assert.assertEquals(getCommandOutput(new String[] {"mkdir",
        "/Complex!@#$%^&*()-_=+[]{};\"'<>," + ".?/File"}), mOutput.toString());
    Assert.assertTrue(status.isFolder());
  }

  @Test
  public void mkdirExisting() throws IOException {
    Assert.assertEquals(0, sFsShell.run("mkdir", "/testFile1"));
    Assert.assertEquals(-1, sFsShell.run("mkdir", "/testFile1"));
  }

  @Test
  public void mkdirPathWithWhiteSpaces() {
    String[] paths = new String[]{
        "/ ",
        "/x y z",
        "/ x y z",
        "/ x y z / a b c"
    };
    for (String path : paths) {
      Assert.assertEquals(0, sFsShell.run("mkdir", path));
    }
  }

  @Test
  public void mkdirInvalidPath() throws IOException {
    Assert.assertEquals(-1, sFsShell.run("mkdir", ""));
  }

  @Test
  public void mkdirMultiPath() throws IOException, AlluxioException {
    String path1 = "/testDir1";
    String path2 = "/testDir2";
    String path3 = "/testDir2/testDir2.1";
    Assert.assertEquals(0, sFsShell.run("mkdir", path1, path2, path3));

    URIStatus status = sFileSystem.getStatus(new AlluxioURI(path1));
    Assert.assertNotNull(status);
    Assert.assertTrue(status.isFolder());

    status = sFileSystem.getStatus(new AlluxioURI(path2));
    Assert.assertNotNull(status);
    Assert.assertTrue(status.isFolder());

    status = sFileSystem.getStatus(new AlluxioURI(path3));
    Assert.assertNotNull(status);
    Assert.assertTrue(status.isFolder());
  }

  @Test
  public void mkdirShortPath() throws IOException, AlluxioException {
    sFsShell.run("mkdir", "/root/testFile1");
    URIStatus status = sFileSystem.getStatus(new AlluxioURI("/root/testFile1"));
    Assert.assertNotNull(status);
    Assert.assertEquals(getCommandOutput(new String[]{"mkdir", "/root/testFile1"}),
        mOutput.toString());
    Assert.assertTrue(status.isFolder());
  }
}
