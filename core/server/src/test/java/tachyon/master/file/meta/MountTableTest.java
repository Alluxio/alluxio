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

package tachyon.master.file.meta;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.TachyonURI;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.InvalidPathException;

/**
 * Unit tests for {@link MountTable}.
 */
public class MountTableTest {
  private MountTable mMountTable;

  /**
   * Sets up a new {@link MountTable} before a test runs.
   */
  @Before
  public void before() {
    mMountTable = new MountTable();
  }

  /**
   * Tests the different methods of the {@link MountTable} class with a path.
   *
   * @throws Exception if a {@link MountTable} operation fails
   */
  @Test
  public void pathTest() throws Exception {
    // Test add()
    mMountTable.add(new TachyonURI("/mnt/foo"), new TachyonURI("/foo"));
    mMountTable.add(new TachyonURI("/mnt/bar"), new TachyonURI("/bar"));

    try {
      mMountTable.add(new TachyonURI("/mnt/foo"), new TachyonURI("/foo2"));
      Assert.fail("Should not be able to add a mount to an existing mount.");
    } catch (FileAlreadyExistsException e) {
      // Exception expected
      Assert.assertEquals(ExceptionMessage.MOUNT_POINT_ALREADY_EXISTS.getMessage("/mnt/foo"),
          e.getMessage());
    }

    try {
      mMountTable.add(new TachyonURI("/mnt/bar/baz"), new TachyonURI("/baz"));
    } catch (InvalidPathException e) {
      // Exception expected
      Assert.assertEquals(ExceptionMessage.MOUNT_POINT_PREFIX_OF_ANOTHER.getMessage("/mnt/bar",
          "/mnt/bar/baz"), e.getMessage());
    }

    // Test resolve()
    Assert.assertEquals(new TachyonURI("/foo"), mMountTable.resolve(new TachyonURI("/mnt/foo")));
    Assert
        .assertEquals(new TachyonURI("/foo/x"), mMountTable.resolve(new TachyonURI("/mnt/foo/x")));
    Assert.assertEquals(new TachyonURI("/bar"), mMountTable.resolve(new TachyonURI("/mnt/bar")));
    Assert
        .assertEquals(new TachyonURI("/bar/y"), mMountTable.resolve(new TachyonURI("/mnt/bar/y")));
    Assert.assertEquals(new TachyonURI("/bar/baz"),
        mMountTable.resolve(new TachyonURI("/mnt/bar/baz")));
    Assert.assertEquals(new TachyonURI("/foobar"), mMountTable.resolve(new TachyonURI("/foobar")));
    Assert.assertEquals(new TachyonURI("/"), mMountTable.resolve(new TachyonURI("/")));

    // Test getMountPoint()
    Assert.assertEquals("/mnt/foo", mMountTable.getMountPoint(new TachyonURI("/mnt/foo")));
    Assert.assertEquals("/mnt/foo", mMountTable.getMountPoint(new TachyonURI("/mnt/foo/x")));
    Assert.assertEquals("/mnt/bar", mMountTable.getMountPoint(new TachyonURI("/mnt/bar")));
    Assert.assertEquals("/mnt/bar", mMountTable.getMountPoint(new TachyonURI("/mnt/bar/y")));
    Assert.assertEquals("/mnt/bar", mMountTable.getMountPoint(new TachyonURI("/mnt/bar/baz")));
    Assert.assertNull(mMountTable.getMountPoint(new TachyonURI("/mnt")));
    Assert.assertNull(mMountTable.getMountPoint(new TachyonURI("/")));

    // Test isMountPoint()
    Assert.assertFalse(mMountTable.isMountPoint(new TachyonURI("/")));
    Assert.assertTrue(mMountTable.isMountPoint(new TachyonURI("/mnt/foo")));
    Assert.assertFalse(mMountTable.isMountPoint(new TachyonURI("/mnt/foo/bar")));
    Assert.assertFalse(mMountTable.isMountPoint(new TachyonURI("/mnt")));
    Assert.assertFalse(mMountTable.isMountPoint(new TachyonURI("/mnt/foo3")));
    Assert.assertTrue(mMountTable.isMountPoint(new TachyonURI("/mnt/bar")));
    Assert.assertFalse(mMountTable.isMountPoint(new TachyonURI("/mnt/bar/baz")));

    // Test delete()
    Assert.assertTrue(mMountTable.delete(new TachyonURI("/mnt/bar")));
    Assert.assertTrue(mMountTable.delete(new TachyonURI("/mnt/foo")));
    Assert.assertFalse(mMountTable.delete(new TachyonURI("/mnt/foo")));
    Assert.assertFalse(mMountTable.delete(new TachyonURI("/")));
  }

  /**
   * Tests the different methods of the {@link MountTable} class with an URI.
   *
   * @throws Exception if a {@link MountTable} operation fails
   */
  @Test
  public void uriTest() throws Exception {
    // Test add()
    mMountTable.add(new TachyonURI("tachyon://localhost:1234/mnt/foo"), new TachyonURI(
        "hdfs://localhost:5678/foo"));
    mMountTable.add(new TachyonURI("tachyon://localhost:1234/mnt/bar"), new TachyonURI(
        "hdfs://localhost:5678/bar"));

    try {
      mMountTable.add(new TachyonURI("tachyon://localhost:1234/mnt/foo"), new TachyonURI(
          "hdfs://localhost:5678/foo2"));
    } catch (FileAlreadyExistsException e) {
      // Exception expected
      Assert.assertEquals(ExceptionMessage.MOUNT_POINT_ALREADY_EXISTS.getMessage("/mnt/foo"),
          e.getMessage());
    }

    try {
      mMountTable.add(new TachyonURI("tachyon://localhost:1234/mnt/bar/baz"), new TachyonURI(
          "hdfs://localhost:5678/baz"));
    } catch (InvalidPathException e) {
      Assert.assertEquals(
          ExceptionMessage.MOUNT_POINT_PREFIX_OF_ANOTHER.getMessage("/mnt/bar", "/mnt/bar/baz"),
          e.getMessage());
    }

    // Test resolve()
    Assert.assertEquals(new TachyonURI("hdfs://localhost:5678/foo"),
        mMountTable.resolve(new TachyonURI("tachyon://localhost:1234/mnt/foo")));
    Assert.assertEquals(new TachyonURI("hdfs://localhost:5678/bar"),
        mMountTable.resolve(new TachyonURI("tachyon://localhost:1234/mnt/bar")));
    Assert.assertEquals(new TachyonURI("hdfs://localhost:5678/bar/y"),
        mMountTable.resolve(new TachyonURI("tachyon://localhost:1234/mnt/bar/y")));
    Assert.assertEquals(new TachyonURI("hdfs://localhost:5678/bar/baz"),
        mMountTable.resolve(new TachyonURI("tachyon://localhost:1234/mnt/bar/baz")));

    // Test getMountPoint()
    Assert.assertEquals("/mnt/foo",
        mMountTable.getMountPoint(new TachyonURI("tachyon://localhost:1234/mnt/foo")));
    Assert.assertEquals("/mnt/bar",
        mMountTable.getMountPoint(new TachyonURI("tachyon://localhost:1234/mnt/bar")));
    Assert.assertEquals("/mnt/bar",
        mMountTable.getMountPoint(new TachyonURI("tachyon://localhost:1234/mnt/bar/y")));
    Assert.assertEquals("/mnt/bar",
        mMountTable.getMountPoint(new TachyonURI("tachyon://localhost:1234/mnt/bar/baz")));
    Assert.assertNull(mMountTable.getMountPoint(new TachyonURI("tachyon://localhost:1234/mnt")));
    Assert.assertNull(mMountTable.getMountPoint(new TachyonURI("tachyon://localhost:1234/")));

    // Test isMountPoint()
    Assert.assertFalse(mMountTable.isMountPoint(new TachyonURI("tachyon://localhost:1234/")));
    Assert.assertTrue(mMountTable.isMountPoint(new TachyonURI("tachyon://localhost:1234/mnt/foo")));
    Assert.assertFalse(
        mMountTable.isMountPoint(new TachyonURI("tachyon://localhost:1234/mnt/foo/bar")));
    Assert.assertFalse(mMountTable.isMountPoint(new TachyonURI("tachyon://localhost:1234/mnt")));
    Assert
        .assertFalse(mMountTable.isMountPoint(new TachyonURI("tachyon://localhost:1234/mnt/foo2")));
    Assert
        .assertFalse(mMountTable.isMountPoint(new TachyonURI("tachyon://localhost:1234/mnt/foo3")));
    Assert.assertTrue(mMountTable.isMountPoint(new TachyonURI("tachyon://localhost:1234/mnt/bar")));
    Assert.assertFalse(
        mMountTable.isMountPoint(new TachyonURI("tachyon://localhost:1234/mnt/bar/baz")));

    // Test delete()
    Assert.assertTrue(mMountTable.delete(new TachyonURI("tachyon://localhost:1234/mnt/bar")));
    Assert.assertTrue(mMountTable.delete(new TachyonURI("tachyon://localhost:1234/mnt/foo")));
    Assert.assertFalse(mMountTable.delete(new TachyonURI("tachyon://localhost:1234/mnt/foo")));
    Assert.assertFalse(mMountTable.delete(new TachyonURI("tachyon://localhost:1234/")));
  }
}
