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

package alluxio.master.file.meta;

import alluxio.AlluxioURI;
import alluxio.exception.AccessControlException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.file.options.MountOptions;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UfsManager.UfsInfo;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.local.LocalUnderFileSystemFactory;
import alluxio.util.IdUtils;

import com.google.common.base.Suppliers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link MountTable}.
 */
public final class MountTableTest {
  private MountTable mMountTable;
  private final MountOptions mDefaultOptions = MountOptions.defaults();
  private final UnderFileSystem mTestUfs =
      new LocalUnderFileSystemFactory().create("/", UnderFileSystemConfiguration.defaults());

  @Before
  public void before() throws Exception {
    UfsManager ufsManager = Mockito.mock(UfsManager.class);
    UfsInfo ufsInfo = new UfsInfo(Suppliers.ofInstance(mTestUfs), AlluxioURI.EMPTY_URI);
    Mockito.when(ufsManager.get(Mockito.anyLong())).thenReturn(ufsInfo);
    mMountTable = new MountTable(ufsManager);
  }

  /**
   * Tests the different methods of the {@link MountTable} class with a path.
   */
  @Test
  public void path() throws Exception {
    // Test add()
    mMountTable.add(new AlluxioURI("/mnt/foo"), new AlluxioURI("/foo"), 1L, mDefaultOptions);
    mMountTable.add(new AlluxioURI("/mnt/bar"), new AlluxioURI("/bar"), 2L, mDefaultOptions);

    try {
      mMountTable.add(new AlluxioURI("/mnt/foo"), new AlluxioURI("/foo2"), 3L, mDefaultOptions);
      Assert.fail("Should not be able to add a mount to an existing mount.");
    } catch (FileAlreadyExistsException e) {
      // Exception expected
      Assert.assertEquals(ExceptionMessage.MOUNT_POINT_ALREADY_EXISTS.getMessage("/mnt/foo"),
          e.getMessage());
    }

    try {
      mMountTable.add(new AlluxioURI("/mnt/bar/baz"), new AlluxioURI("/baz"), 4L,
          mDefaultOptions);
    } catch (InvalidPathException e) {
      // Exception expected
      Assert.assertEquals(
          ExceptionMessage.MOUNT_POINT_PREFIX_OF_ANOTHER.getMessage("/mnt/bar", "/mnt/bar/baz"),
          e.getMessage());
    }

    // Test resolve()
    MountTable.Resolution res1 = mMountTable.resolve(new AlluxioURI("/mnt/foo"));
    Assert.assertEquals(new AlluxioURI("/foo"), res1.getUri());
    Assert.assertEquals(1L, res1.getMountId());
    MountTable.Resolution res2 = mMountTable.resolve(new AlluxioURI("/mnt/foo/x"));
    Assert.assertEquals(new AlluxioURI("/foo/x"), res2.getUri());
    Assert.assertEquals(1L, res2.getMountId());
    MountTable.Resolution res3 = mMountTable.resolve(new AlluxioURI("/mnt/bar"));
    Assert.assertEquals(new AlluxioURI("/bar"), res3.getUri());
    Assert.assertEquals(2L, res3.getMountId());
    MountTable.Resolution res4 = mMountTable.resolve(new AlluxioURI("/mnt/bar/y"));
    Assert.assertEquals(new AlluxioURI("/bar/y"), res4.getUri());
    Assert.assertEquals(2L, res4.getMountId());
    MountTable.Resolution res5 = mMountTable.resolve(new AlluxioURI("/mnt/bar/baz"));
    Assert.assertEquals(new AlluxioURI("/bar/baz"), res5.getUri());
    Assert.assertEquals(2L, res4.getMountId());
    MountTable.Resolution res6 = mMountTable.resolve(new AlluxioURI("/foobar"));
    Assert.assertEquals(new AlluxioURI("/foobar"), res6.getUri());
    Assert.assertEquals(IdUtils.INVALID_MOUNT_ID, res6.getMountId());
    MountTable.Resolution res7 = mMountTable.resolve(new AlluxioURI("/"));
    Assert.assertEquals(new AlluxioURI("/"), res7.getUri());
    Assert.assertEquals(IdUtils.INVALID_MOUNT_ID, res7.getMountId());

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
   * Tests the different methods of the {@link MountTable} class with a URI.
   */
  @Test
  public void uri() throws Exception {
    // Test add()
    mMountTable.add(new AlluxioURI("alluxio://localhost:1234/mnt/foo"),
        new AlluxioURI("file://localhost:5678/foo"), 1L, mDefaultOptions);
    mMountTable.add(new AlluxioURI("alluxio://localhost:1234/mnt/bar"),
        new AlluxioURI("file://localhost:5678/bar"), 2L, mDefaultOptions);

    try {
      mMountTable.add(new AlluxioURI("alluxio://localhost:1234/mnt/foo"),
          new AlluxioURI("hdfs://localhost:5678/foo2"), 3L, mDefaultOptions);
    } catch (FileAlreadyExistsException e) {
      // Exception expected
      Assert.assertEquals(ExceptionMessage.MOUNT_POINT_ALREADY_EXISTS.getMessage("/mnt/foo"),
          e.getMessage());
    }

    try {
      mMountTable.add(new AlluxioURI("alluxio://localhost:1234/mnt/bar/baz"),
          new AlluxioURI("hdfs://localhost:5678/baz"), 4L, mDefaultOptions);
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
   */
  @Test
  public void readOnlyMount() throws Exception {
    MountOptions options = MountOptions.defaults().setReadOnly(true);
    String mountPath = "/mnt/foo";
    AlluxioURI alluxioUri = new AlluxioURI("alluxio://localhost:1234" + mountPath);
    mMountTable.add(alluxioUri, new AlluxioURI("hdfs://localhost:5678/foo"), 1L, options);

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
   */
  @Test
  public void writableMount() throws Exception {
    String mountPath = "/mnt/foo";
    AlluxioURI alluxioUri = new AlluxioURI("alluxio://localhost:1234" + mountPath);
    mMountTable
        .add(alluxioUri, new AlluxioURI("hdfs://localhost:5678/foo"), IdUtils.INVALID_MOUNT_ID,
            MountOptions.defaults());

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

  /**
   * Tests the method for getting a copy of the current mount table.
   */
  @Test
  public void getMountTable() throws Exception {
    Map<String, MountInfo> mountTable = new HashMap<>(2);
    mountTable.put("/mnt/foo",
        new MountInfo(new AlluxioURI("/mnt/foo"), new AlluxioURI("hdfs://localhost:5678/foo"), 1L,
            MountOptions.defaults()));
    mountTable.put("/mnt/bar",
        new MountInfo(new AlluxioURI("/mnt/bar"), new AlluxioURI("hdfs://localhost:5678/bar"), 2L,
            MountOptions.defaults()));

    AlluxioURI masterAddr = new AlluxioURI("alluxio://localhost:1234");
    for (Map.Entry<String, MountInfo> mountPoint : mountTable.entrySet()) {
      MountInfo mountInfo = mountPoint.getValue();
      mMountTable.add(masterAddr.join(mountPoint.getKey()),
          mountInfo.getUfsUri(), mountInfo.getMountId(), mountInfo.getOptions());
    }
    Assert.assertEquals(mountTable, mMountTable.getMountTable());
  }

  /**
   * Tests the method for getting mount info given mount id.
   */
  @Test
  public void getMountInfo() throws Exception {
    MountInfo info1 =
        new MountInfo(new AlluxioURI("/mnt/foo"), new AlluxioURI("hdfs://localhost:5678/foo"), 1L,
            MountOptions.defaults());
    MountInfo info2 =
        new MountInfo(new AlluxioURI("/mnt/bar"), new AlluxioURI("hdfs://localhost:5678/bar"), 2L,
            MountOptions.defaults());
    mMountTable
        .add(new AlluxioURI("/mnt/foo"), info1.getUfsUri(), info1.getMountId(), info1.getOptions());
    mMountTable
        .add(new AlluxioURI("/mnt/bar"), info2.getUfsUri(), info2.getMountId(), info2.getOptions());
    Assert.assertEquals(info1, mMountTable.getMountInfo(info1.getMountId()));
    Assert.assertEquals(info2, mMountTable.getMountInfo(info2.getMountId()));
    Assert.assertEquals(null, mMountTable.getMountInfo(3L));
  }
}
