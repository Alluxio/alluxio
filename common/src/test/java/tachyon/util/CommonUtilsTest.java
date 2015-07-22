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

package tachyon.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import tachyon.Constants;
import tachyon.thrift.InvalidPathException;

public class CommonUtilsTest {
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();
  @Rule
  public final ExpectedException mException = ExpectedException.none();

  /**
   * Test for both changeLocalFilePermission and changeLocalFileToFullPermission
   *
   * @param filePath that will change permission
   * @throws IOException
   */

  @Test
  public void changeLocalFilePermissionTest() throws IOException {
    File tempFile = mTestFolder.newFile("perm.txt");
    CommonUtils.changeLocalFilePermission(tempFile.getAbsolutePath(), "000");
    Assert.assertFalse(tempFile.canRead() && tempFile.canWrite() && tempFile.canExecute());
    CommonUtils.changeLocalFilePermission(tempFile.getAbsolutePath(), "777");
    Assert.assertTrue(tempFile.canRead() && tempFile.canWrite() && tempFile.canExecute());
    // File deletion should fail, because we don't have write permissions
    CommonUtils.changeLocalFilePermission(tempFile.getAbsolutePath(), "444");
    Assert.assertTrue(tempFile.canRead());
    Assert.assertFalse(tempFile.canWrite());
    Assert.assertFalse(tempFile.canExecute());
    // expect a file permission error when we open it for writing
    mException.expect(IOException.class);
    FileWriter fw = new FileWriter(tempFile);
    if (fw != null) {
      fw.close();
    }

    // Change permission on directories
    CommonUtils.changeLocalFilePermission(mTestFolder.getRoot().getAbsolutePath(), "444");
    Assert.assertFalse(tempFile.delete());
    CommonUtils.changeLocalFilePermission(mTestFolder.getRoot().getAbsolutePath(), "744");
    Assert.assertTrue(tempFile.delete());

    // tempFile no longer exist, so changing permission on it should return exception
    mException.expect(IOException.class);
    CommonUtils.changeLocalFilePermission(tempFile.getAbsolutePath(), "777");
  }

  @Test
  public void getPathComponentsTest() throws InvalidPathException {
    try {
      Assert.assertArrayEquals(new String[] {""}, CommonUtils.getPathComponents("/"));

      Assert.assertArrayEquals(new String[] {"", "bar"}, CommonUtils.getPathComponents("/bar"));
      Assert.assertArrayEquals(new String[] {"", "foo", "bar"},
          CommonUtils.getPathComponents("/foo/bar"));
      Assert.assertArrayEquals(new String[] {"", "foo", "bar"},
          CommonUtils.getPathComponents("/foo/bar/"));
      Assert.assertArrayEquals(new String[] {"", "bar"},
          CommonUtils.getPathComponents("/foo/../bar"));
      Assert.assertArrayEquals(new String[] {"", "foo", "bar", "a", "b", "c"},
          CommonUtils.getPathComponents("/foo//bar/a/b/c"));

    } catch (InvalidPathException e) {
      Assert.fail();
    }
    mException.expect(InvalidPathException.class);
    CommonUtils.getPathComponents("/\\   foo / bar");
  }

  @Test
  public void cloneByteBufferTest() {
    final int bufferSize = 10;
    ByteBuffer buf = ByteBuffer.allocate(bufferSize);
    ByteBuffer bufDirect = ByteBuffer.allocateDirect(bufferSize);
    ByteBuffer bufClone = null;
    for (byte i = 0; i < bufferSize; i ++) {
      buf.put(i);
      bufDirect.put((byte) (i + 1));
    }
    bufClone = CommonUtils.cloneByteBuffer(buf);
    Assert.assertTrue(bufClone.equals(buf));
    bufClone = CommonUtils.cloneByteBuffer(bufDirect);
    Assert.assertTrue(bufClone.equals(bufDirect));
  }

  @Test
  public void parseSpaceSizeTest() {
    long max = 10240;
    for (long k = 0; k < max; k ++) {
      Assert.assertEquals(k / 10, CommonUtils.parseSpaceSize(k / 10.0 + "b"));
      Assert.assertEquals(k / 10, CommonUtils.parseSpaceSize(k / 10.0 + "B"));
      Assert.assertEquals(k / 10, CommonUtils.parseSpaceSize(k / 10.0 + ""));
    }
    for (long k = 0; k < max; k ++) {
      Assert.assertEquals(k * Constants.KB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "kb"));
      Assert.assertEquals(k * Constants.KB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "Kb"));
      Assert.assertEquals(k * Constants.KB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "KB"));
      Assert.assertEquals(k * Constants.KB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "kB"));
    }
    for (long k = 0; k < max; k ++) {
      Assert.assertEquals(k * Constants.MB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "mb"));
      Assert.assertEquals(k * Constants.MB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "Mb"));
      Assert.assertEquals(k * Constants.MB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "MB"));
      Assert.assertEquals(k * Constants.MB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "mB"));
    }
    for (long k = 0; k < max; k ++) {
      Assert.assertEquals(k * Constants.GB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "gb"));
      Assert.assertEquals(k * Constants.GB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "Gb"));
      Assert.assertEquals(k * Constants.GB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "GB"));
      Assert.assertEquals(k * Constants.GB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "gB"));
    }
    for (long k = 0; k < max; k ++) {
      Assert.assertEquals(k * Constants.TB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "tb"));
      Assert.assertEquals(k * Constants.TB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "Tb"));
      Assert.assertEquals(k * Constants.TB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "TB"));
      Assert.assertEquals(k * Constants.TB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "tB"));
    }
    // We stop the pb test before 8192, since 8192 petabytes is beyond the scope of a java long.
    for (long k = 0; k < 8192; k ++) {
      Assert.assertEquals(k * Constants.PB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "pb"));
      Assert.assertEquals(k * Constants.PB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "Pb"));
      Assert.assertEquals(k * Constants.PB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "PB"));
      Assert.assertEquals(k * Constants.PB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "pB"));
    }
  }

  @Test
  public void concatPathTest() {
    Assert.assertEquals("", CommonUtils.concatPath());
    Assert.assertEquals("/bar", CommonUtils.concatPath("/", "bar"));

    Assert.assertEquals("foo", CommonUtils.concatPath("foo"));
    Assert.assertEquals("/foo", CommonUtils.concatPath("/foo"));
    Assert.assertEquals("/foo", CommonUtils.concatPath("/foo", ""));

    // Join base without trailing "/"
    Assert.assertEquals("/foo/bar", CommonUtils.concatPath("/foo", "bar"));
    Assert.assertEquals("/foo/bar", CommonUtils.concatPath("/foo", "bar/"));
    Assert.assertEquals("/foo/bar", CommonUtils.concatPath("/foo", "/bar"));
    Assert.assertEquals("/foo/bar", CommonUtils.concatPath("/foo", "/bar/"));

    // Join base with trailing "/"
    Assert.assertEquals("/foo/bar", CommonUtils.concatPath("/foo/", "bar"));
    Assert.assertEquals("/foo/bar", CommonUtils.concatPath("/foo/", "bar/"));
    Assert.assertEquals("/foo/bar", CommonUtils.concatPath("/foo/", "/bar"));
    Assert.assertEquals("/foo/bar", CommonUtils.concatPath("/foo/", "/bar/"));

    // Whitespace must be trimmed.
    Assert.assertEquals("/foo/bar", CommonUtils.concatPath("/foo ", "bar  "));
    Assert.assertEquals("/foo/bar", CommonUtils.concatPath("/foo ", "  bar"));
    Assert.assertEquals("/foo/bar", CommonUtils.concatPath("/foo ", "  bar  "));

    // Redundant separator must be trimmed.
    Assert.assertEquals("/foo/bar", CommonUtils.concatPath("/foo/", "bar//"));

    // Multiple components to join.
    Assert.assertEquals("/foo/bar/a/b/c", CommonUtils.concatPath("/foo", "bar", "a", "b", "c"));
    Assert.assertEquals("/foo/bar/b/c", CommonUtils.concatPath("/foo", "bar", "", "b", "c"));

    // Non-string
    Assert.assertEquals("/foo/bar/1", CommonUtils.concatPath("/foo", "bar", 1));
    Assert.assertEquals("/foo/bar/2", CommonUtils.concatPath("/foo", "bar", 2L));
  }
}
