/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file.meta;

import alluxio.AlluxioURI;
import alluxio.exception.AccessControlException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.options.MountOptions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link MountTable}.
 */
public class MountTableTest {
  private MountTable mMountTable;
  private final MountOptions mDefaultOptions = MountOptions.defaults();

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
    mMountTable.add(new AlluxioURI("/mnt/foo"), new AlluxioURI("/foo"), mDefaultOptions);
    mMountTable.add(new AlluxioURI("/mnt/bar"), new AlluxioURI("/bar"), mDefaultOptions);

    try {
      mMountTable.add(new AlluxioURI("/mnt/foo"), new AlluxioURI("/foo2"), mDefaultOptions);
      Assert.fail("Should not be able to add a mount to an existing mount.");
    } catch (FileAlreadyExistsException e) {
      // Exception expected
      Assert.assertEquals(ExceptionMessage.MOUNT_POINT_ALREADY_EXISTS.getMessage("/mnt/foo"),
          e.getMessage());
    }

    try {
      mMountTable.add(new AlluxioURI("/mnt/bar/baz"), new AlluxioURI("/baz"), mDefaultOptions);
    } catch (InvalidPathException e) {
      // Exception expected
      Assert.assertEquals(
          ExceptionMessage.MOUNT_POINT_PREFIX_OF_ANOTHER.getMessage("/mnt/bar", "/mnt/bar/baz"),
          e.getMessage());
    }

    // Test resolve()
    Assert.assertEquals(new AlluxioURI("/foo"),
        mMountTable.resolve(new AlluxioURI("/mnt/foo")).getUri());
    Assert.assertEquals(new AlluxioURI("/foo/x"),
        mMountTable.resolve(new AlluxioURI("/mnt/foo/x")).getUri());
    Assert.assertEquals(new AlluxioURI("/bar"),
        mMountTable.resolve(new AlluxioURI("/mnt/bar")).getUri());
    Assert.assertEquals(new AlluxioURI("/bar/y"),
        mMountTable.resolve(new AlluxioURI("/mnt/bar/y")).getUri());
    Assert.assertEquals(new AlluxioURI("/bar/baz"),
        mMountTable.resolve(new AlluxioURI("/mnt/bar/baz")).getUri());
    Assert.assertEquals(new AlluxioURI("/foobar"),
        mMountTable.resolve(new AlluxioURI("/foobar")).getUri());
    Assert.assertEquals(new AlluxioURI("/"), mMountTable.resolve(new AlluxioURI("/")).getUri());

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
    mMountTable.add(new AlluxioURI("alluxio://localhost:1234/mnt/foo"),
        new AlluxioURI("file://localhost:5678/foo"), mDefaultOptions);
    mMountTable.add(new AlluxioURI("alluxio://localhost:1234/mnt/bar"),
        new AlluxioURI("file://localhost:5678/bar"), mDefaultOptions);

    try {
      mMountTable.add(new AlluxioURI("alluxio://localhost:1234/mnt/foo"),
          new AlluxioURI("hdfs://localhost:5678/foo2"), mDefaultOptions);
    } catch (FileAlreadyExistsException e) {
      // Exception expected
      Assert.assertEquals(ExceptionMessage.MOUNT_POINT_ALREADY_EXISTS.getMessage("/mnt/foo"),
          e.getMessage());
    }

    try {
      mMountTable.add(new AlluxioURI("alluxio://localhost:1234/mnt/bar/baz"),
          new AlluxioURI("hdfs://localhost:5678/baz"), mDefaultOptions);
    } catch (InvalidPathException e) {
      Assert.assertEquals(
          ExceptionMessage.MOUNT_POINT_PREFIX_OF_ANOTHER.getMessage("/mnt/bar", "/mnt/bar/baz"),
          e.getMessage());
    }

    // Test resolve()
    Assert.assertEquals(new AlluxioURI("file://localhost:5678/foo"),
        mMountTable.resolve(new AlluxioURI("alluxio://localhost:1234/mnt/foo")).getUri());
    Assert.assertEquals(new AlluxioURI("file://localhost:5678/bar"),
        mMountTable.resolve(new AlluxioURI("alluxio://localhost:1234/mnt/bar")).getUri());
    Assert.assertEquals(new AlluxioURI("file://localhost:5678/bar/y"),
        mMountTable.resolve(new AlluxioURI("alluxio://localhost:1234/mnt/bar/y")).getUri());
    Assert.assertEquals(new AlluxioURI("file://localhost:5678/bar/baz"),
        mMountTable.resolve(new AlluxioURI("alluxio://localhost:1234/mnt/bar/baz")).getUri());

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

  /**
   * Tests check of readonly mount points.
   *
   * @throws Exception if a {@link MountTable} operation fails
   */
  @Test
  public void readOnlyMountTest() throws Exception {
    MountOptions options = MountOptions.defaults().setReadOnly(true);
    String mountPath = "/mnt/foo";
    AlluxioURI alluxioUri = new AlluxioURI("alluxio://localhost:1234" + mountPath);
    mMountTable.add(alluxioUri, new AlluxioURI("hdfs://localhost:5678/foo"), options);

    try {
      mMountTable.checkUnderWritableMountPoint(alluxioUri);
      Assert.fail("Readonly mount point should not be writable.");
    } catch (AccessControlException e) {
      // Exception expected
      Assert.assertEquals(ExceptionMessage.MOUNT_READONLY.getMessage(alluxioUri, mountPath),
          e.getMessage());
    }

    try {
      String path = mountPath + "/sub/directory";
      alluxioUri = new AlluxioURI("alluxio://localhost:1234" + path);
      mMountTable.checkUnderWritableMountPoint(alluxioUri);
      Assert.fail("Readonly mount point should not be writable.");
    } catch (AccessControlException e) {
      // Exception expected
      Assert.assertEquals(ExceptionMessage.MOUNT_READONLY.getMessage(alluxioUri, mountPath),
          e.getMessage());
    }
  }

  /**
   * Tests check of writable mount points.
   *
   * @throws Exception if a {@link MountTable} operation fails
   */
  @Test
  public void writableMountTest() throws Exception {
    String mountPath = "/mnt/foo";
    AlluxioURI alluxioUri = new AlluxioURI("alluxio://localhost:1234" + mountPath);
    mMountTable
        .add(alluxioUri, new AlluxioURI("hdfs://localhost:5678/foo"), MountOptions.defaults());

    try {
      mMountTable.checkUnderWritableMountPoint(alluxioUri);
    } catch (AccessControlException e) {
      Assert.fail("Default mount point should be writable.");
    }

    try {
      String path = mountPath + "/sub/directory";
      alluxioUri = new AlluxioURI("alluxio://localhost:1234" + path);
      mMountTable.checkUnderWritableMountPoint(alluxioUri);
    } catch (AccessControlException e) {
      Assert.fail("Default mount point should be writable.");
    }
  }
}
