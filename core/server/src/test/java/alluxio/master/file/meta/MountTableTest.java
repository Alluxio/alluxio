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

package alluxio.master.file.meta;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import alluxio.AlluxioURI;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.InvalidPathException;

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
    mMountTable.add(new AlluxioURI("/mnt/foo"), new AlluxioURI("/foo"));
    mMountTable.add(new AlluxioURI("/mnt/bar"), new AlluxioURI("/bar"));

    try {
      mMountTable.add(new AlluxioURI("/mnt/foo"), new AlluxioURI("/foo2"));
      Assert.fail("Should not be able to add a mount to an existing mount.");
    } catch (FileAlreadyExistsException e) {
      // Exception expected
      Assert.assertEquals(ExceptionMessage.MOUNT_POINT_ALREADY_EXISTS.getMessage("/mnt/foo"),
          e.getMessage());
    }

    try {
      mMountTable.add(new AlluxioURI("/mnt/bar/baz"), new AlluxioURI("/baz"));
    } catch (InvalidPathException e) {
      // Exception expected
      Assert.assertEquals(ExceptionMessage.MOUNT_POINT_PREFIX_OF_ANOTHER.getMessage("/mnt/bar",
          "/mnt/bar/baz"), e.getMessage());
    }

    // Test resolve()
    Assert.assertEquals(new AlluxioURI("/foo"), mMountTable.resolve(new AlluxioURI("/mnt/foo")));
    Assert
        .assertEquals(new AlluxioURI("/foo/x"), mMountTable.resolve(new AlluxioURI("/mnt/foo/x")));
    Assert.assertEquals(new AlluxioURI("/bar"), mMountTable.resolve(new AlluxioURI("/mnt/bar")));
    Assert
        .assertEquals(new AlluxioURI("/bar/y"), mMountTable.resolve(new AlluxioURI("/mnt/bar/y")));
    Assert.assertEquals(new AlluxioURI("/bar/baz"),
        mMountTable.resolve(new AlluxioURI("/mnt/bar/baz")));
    Assert.assertEquals(new AlluxioURI("/foobar"), mMountTable.resolve(new AlluxioURI("/foobar")));
    Assert.assertEquals(new AlluxioURI("/"), mMountTable.resolve(new AlluxioURI("/")));

    // Test getMountPoint()
    Assert.assertEquals("/mnt/foo", mMountTable.getMountPoint(new AlluxioURI("/mnt/foo")));
    Assert.assertEquals("/mnt/foo", mMountTable.getMountPoint(new AlluxioURI("/mnt/foo/x")));
    Assert.assertEquals("/mnt/bar", mMountTable.getMountPoint(new AlluxioURI("/mnt/bar")));
    Assert.assertEquals("/mnt/bar", mMountTable.getMountPoint(new AlluxioURI("/mnt/bar/y")));
    Assert.assertEquals("/mnt/bar", mMountTable.getMountPoint(new AlluxioURI("/mnt/bar/baz")));
    Assert.assertNull(mMountTable.getMountPoint(new AlluxioURI("/mnt")));
    Assert.assertNull(mMountTable.getMountPoint(new AlluxioURI("/")));

    // Test isMountPoint()
    Assert.assertFalse(mMountTable.isMountPoint(new AlluxioURI("/")));
    Assert.assertTrue(mMountTable.isMountPoint(new AlluxioURI("/mnt/foo")));
    Assert.assertFalse(mMountTable.isMountPoint(new AlluxioURI("/mnt/foo/bar")));
    Assert.assertFalse(mMountTable.isMountPoint(new AlluxioURI("/mnt")));
    Assert.assertFalse(mMountTable.isMountPoint(new AlluxioURI("/mnt/foo3")));
    Assert.assertTrue(mMountTable.isMountPoint(new AlluxioURI("/mnt/bar")));
    Assert.assertFalse(mMountTable.isMountPoint(new AlluxioURI("/mnt/bar/baz")));

    // Test delete()
    Assert.assertTrue(mMountTable.delete(new AlluxioURI("/mnt/bar")));
    Assert.assertTrue(mMountTable.delete(new AlluxioURI("/mnt/foo")));
    Assert.assertFalse(mMountTable.delete(new AlluxioURI("/mnt/foo")));
    Assert.assertFalse(mMountTable.delete(new AlluxioURI("/")));
  }

  /**
   * Tests the different methods of the {@link MountTable} class with an URI.
   *
   * @throws Exception if a {@link MountTable} operation fails
   */
  @Test
  public void uriTest() throws Exception {
    // Test add()
    mMountTable.add(new AlluxioURI("alluxio://localhost:1234/mnt/foo"), new AlluxioURI(
        "hdfs://localhost:5678/foo"));
    mMountTable.add(new AlluxioURI("alluxio://localhost:1234/mnt/bar"), new AlluxioURI(
        "hdfs://localhost:5678/bar"));

    try {
      mMountTable.add(new AlluxioURI("alluxio://localhost:1234/mnt/foo"), new AlluxioURI(
          "hdfs://localhost:5678/foo2"));
    } catch (FileAlreadyExistsException e) {
      // Exception expected
      Assert.assertEquals(ExceptionMessage.MOUNT_POINT_ALREADY_EXISTS.getMessage("/mnt/foo"),
          e.getMessage());
    }

    try {
      mMountTable.add(new AlluxioURI("alluxio://localhost:1234/mnt/bar/baz"), new AlluxioURI(
          "hdfs://localhost:5678/baz"));
    } catch (InvalidPathException e) {
      Assert.assertEquals(
          ExceptionMessage.MOUNT_POINT_PREFIX_OF_ANOTHER.getMessage("/mnt/bar", "/mnt/bar/baz"),
          e.getMessage());
    }

    // Test resolve()
    Assert.assertEquals(new AlluxioURI("hdfs://localhost:5678/foo"),
        mMountTable.resolve(new AlluxioURI("alluxio://localhost:1234/mnt/foo")));
    Assert.assertEquals(new AlluxioURI("hdfs://localhost:5678/bar"),
        mMountTable.resolve(new AlluxioURI("alluxio://localhost:1234/mnt/bar")));
    Assert.assertEquals(new AlluxioURI("hdfs://localhost:5678/bar/y"),
        mMountTable.resolve(new AlluxioURI("alluxio://localhost:1234/mnt/bar/y")));
    Assert.assertEquals(new AlluxioURI("hdfs://localhost:5678/bar/baz"),
        mMountTable.resolve(new AlluxioURI("alluxio://localhost:1234/mnt/bar/baz")));

    // Test getMountPoint()
    Assert.assertEquals("/mnt/foo",
        mMountTable.getMountPoint(new AlluxioURI("alluxio://localhost:1234/mnt/foo")));
    Assert.assertEquals("/mnt/bar",
        mMountTable.getMountPoint(new AlluxioURI("alluxio://localhost:1234/mnt/bar")));
    Assert.assertEquals("/mnt/bar",
        mMountTable.getMountPoint(new AlluxioURI("alluxio://localhost:1234/mnt/bar/y")));
    Assert.assertEquals("/mnt/bar",
        mMountTable.getMountPoint(new AlluxioURI("alluxio://localhost:1234/mnt/bar/baz")));
    Assert.assertNull(mMountTable.getMountPoint(new AlluxioURI("alluxio://localhost:1234/mnt")));
    Assert.assertNull(mMountTable.getMountPoint(new AlluxioURI("alluxio://localhost:1234/")));

    // Test isMountPoint()
    Assert.assertFalse(mMountTable.isMountPoint(new AlluxioURI("alluxio://localhost:1234/")));
    Assert.assertTrue(mMountTable.isMountPoint(new AlluxioURI("alluxio://localhost:1234/mnt/foo")));
    Assert.assertFalse(
        mMountTable.isMountPoint(new AlluxioURI("alluxio://localhost:1234/mnt/foo/bar")));
    Assert.assertFalse(mMountTable.isMountPoint(new AlluxioURI("alluxio://localhost:1234/mnt")));
    Assert
        .assertFalse(mMountTable.isMountPoint(new AlluxioURI("alluxio://localhost:1234/mnt/foo2")));
    Assert
        .assertFalse(mMountTable.isMountPoint(new AlluxioURI("alluxio://localhost:1234/mnt/foo3")));
    Assert.assertTrue(mMountTable.isMountPoint(new AlluxioURI("alluxio://localhost:1234/mnt/bar")));
    Assert.assertFalse(
        mMountTable.isMountPoint(new AlluxioURI("alluxio://localhost:1234/mnt/bar/baz")));

    // Test delete()
    Assert.assertTrue(mMountTable.delete(new AlluxioURI("alluxio://localhost:1234/mnt/bar")));
    Assert.assertTrue(mMountTable.delete(new AlluxioURI("alluxio://localhost:1234/mnt/foo")));
    Assert.assertFalse(mMountTable.delete(new AlluxioURI("alluxio://localhost:1234/mnt/foo")));
    Assert.assertFalse(mMountTable.delete(new AlluxioURI("alluxio://localhost:1234/")));
  }
}
