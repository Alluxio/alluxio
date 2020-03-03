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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import alluxio.client.file.FileSystemTestUtils;
import alluxio.exception.AlluxioException;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.cli.fs.FileSystemShellUtilsTest;
import alluxio.grpc.WritePType;

import org.junit.Test;

import java.io.IOException;

/**
 * Tests for free command.
 */
public final class FreeCommandIntegrationTest extends AbstractFileSystemShellTest {

  @Test
  public void freeNonPersistedFile() throws IOException, AlluxioException {
    String fileName = "/testFile";
    FileSystemTestUtils.createByteFile(sFileSystem, fileName, WritePType.MUST_CACHE, 10);
    // freeing non persisted files is expected to fail
    assertEquals(-1, sFsShell.run("free", fileName));
    assertTrue(isInMemoryTest(fileName));
  }

  @Test
  public void freePinnedFile() throws IOException, AlluxioException {
    String fileName = "/testFile";
    FileSystemTestUtils.createByteFile(sFileSystem, fileName, WritePType.CACHE_THROUGH, 10);
    sFsShell.run("pin", fileName);
    // freeing non persisted files is expected to fail
    assertEquals(-1, sFsShell.run("free", fileName));
    assertTrue(isInMemoryTest(fileName));
  }

  @Test
  public void freePinnedFileForced() throws IOException, AlluxioException {
    String fileName = "/testFile";
    FileSystemTestUtils.createByteFile(sFileSystem, fileName, WritePType.CACHE_THROUGH, 10);
    sFsShell.run("pin", fileName);
    assertEquals(0, sFsShell.run("free", "-f", fileName));
    assertFalse(isInMemoryTest(fileName));
  }

  @Test
  public void free() throws IOException, AlluxioException {
    String fileName = "/testFile";
    FileSystemTestUtils.createByteFile(sFileSystem, fileName, WritePType.CACHE_THROUGH, 10);
    assertEquals(0, sFsShell.run("free", fileName));
    assertFalse(isInMemoryTest(fileName));
  }

  @Test
  public void freeWildCardNonPersistedFile() throws IOException, AlluxioException {
    String testDir =
        FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem, WritePType.MUST_CACHE);
    assertEquals(-1, sFsShell.run("free", testDir + "/foo/*"));
    // freeing non persisted files is expected to fail
    assertTrue(isInMemoryTest(testDir + "/foo/foobar1"));
    assertTrue(isInMemoryTest(testDir + "/foo/foobar2"));
    assertTrue(isInMemoryTest(testDir + "/bar/foobar3"));
    assertTrue(isInMemoryTest(testDir + "/foobar4"));
  }

  @Test
  public void freeWildCardPinnedFile() throws IOException, AlluxioException {
    String testDir =
        FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem, WritePType.CACHE_THROUGH);
    sFsShell.run("pin", testDir + "/foo/*");
    assertEquals(-1, sFsShell.run("free", testDir + "/foo/*"));
    // freeing non pinned files is expected to fail without "-f"
    assertTrue(isInMemoryTest(testDir + "/foo/foobar1"));
    assertTrue(isInMemoryTest(testDir + "/foo/foobar2"));
  }

  @Test
  public void freeWildCardPinnedFileForced() throws IOException, AlluxioException {
    String testDir =
        FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem, WritePType.CACHE_THROUGH);
    sFsShell.run("pin", testDir + "/foo/foobar1");
    assertEquals(0, sFsShell.run("free", "-f", testDir + "/foo/*"));
    assertFalse(isInMemoryTest(testDir + "/foo/foobar1"));
    assertFalse(isInMemoryTest(testDir + "/foo/foobar2"));
    assertTrue(isInMemoryTest(testDir + "/bar/foobar3"));
    assertTrue(isInMemoryTest(testDir + "/foobar4"));
  }

  @Test
  public void freeWildCard() throws IOException, AlluxioException {
    String testDir =
        FileSystemShellUtilsTest.resetFileHierarchy(sFileSystem, WritePType.CACHE_THROUGH);
    int ret = sFsShell.run("free", testDir + "/foo/*");
    assertEquals(0, ret);
    assertFalse(isInMemoryTest(testDir + "/foo/foobar1"));
    assertFalse(isInMemoryTest(testDir + "/foo/foobar2"));
    assertTrue(isInMemoryTest(testDir + "/bar/foobar3"));
    assertTrue(isInMemoryTest(testDir + "/foobar4"));

    ret = sFsShell.run("free", testDir + "/*/");
    assertEquals(0, ret);
    assertFalse(isInMemoryTest(testDir + "/bar/foobar3"));
    assertFalse(isInMemoryTest(testDir + "/foobar4"));
  }
}
