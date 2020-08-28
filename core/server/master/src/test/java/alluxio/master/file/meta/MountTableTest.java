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

import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AccessControlException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.MountPOptions;
import alluxio.master.file.contexts.MountContext;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.journal.NoopJournalContext;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UfsManager.UfsClient;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.local.LocalUnderFileSystemFactory;
import alluxio.util.IdUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link MountTable}.
 */
public final class MountTableTest {
  private MountTable mMountTable;
  private static final String ROOT_UFS = "s3a://bucket/";
  private final UnderFileSystem mTestUfs = new LocalUnderFileSystemFactory().create("/",
      UnderFileSystemConfiguration.defaults(ServerConfiguration.global()));

  @Before
  public void before() throws Exception {
    UfsManager ufsManager = mock(UfsManager.class);
    UfsClient ufsClient =
        new UfsManager.UfsClient(() -> mTestUfs, AlluxioURI.EMPTY_URI);
    when(ufsManager.get(anyLong())).thenReturn(ufsClient);
    mMountTable = new MountTable(ufsManager,
        new MountInfo(new AlluxioURI(MountTable.ROOT), new AlluxioURI(ROOT_UFS),
            IdUtils.ROOT_MOUNT_ID, MountContext.defaults().getOptions().build()));
  }

  /**
   * Tests the different methods of the {@link MountTable} class with a path.
   */
  @Test
  public void path() throws Exception {
    // Test add()
    addMount("/mnt/foo", "/foo", 2);
    addMount("/mnt/bar", "/bar", 3);

    try {
      addMount("/mnt/foo", "/foo2", 4);
      Assert.fail("Should not be able to add a mount to an existing mount.");
    } catch (FileAlreadyExistsException e) {
      // Exception expected
      Assert.assertEquals(ExceptionMessage.MOUNT_POINT_ALREADY_EXISTS.getMessage("/mnt/foo"),
          e.getMessage());
    }

    try {
      addMount("/test1", "hdfs://localhost", 6);
      addMount("/test2", "hdfs://localhost", 7);
      Assert.fail("mount fails");
    } catch (InvalidPathException e) {
      // Exception expected
      Assert.assertEquals(ExceptionMessage.MOUNT_POINT_PREFIX_OF_ANOTHER
          .getMessage("hdfs://localhost", "hdfs://localhost"), e.getMessage());
    }

    // Test resolve()
    MountTable.Resolution res1 = mMountTable.resolve(new AlluxioURI("/mnt/foo"));
    Assert.assertEquals(new AlluxioURI("/foo"), res1.getUri());
    Assert.assertEquals(2L, res1.getMountId());
    MountTable.Resolution res2 = mMountTable.resolve(new AlluxioURI("/mnt/foo/x"));
    Assert.assertEquals(new AlluxioURI("/foo/x"), res2.getUri());
    Assert.assertEquals(2L, res2.getMountId());
    MountTable.Resolution res3 = mMountTable.resolve(new AlluxioURI("/mnt/bar"));
    Assert.assertEquals(new AlluxioURI("/bar"), res3.getUri());
    Assert.assertEquals(3L, res3.getMountId());
    MountTable.Resolution res4 = mMountTable.resolve(new AlluxioURI("/mnt/bar/y"));
    Assert.assertEquals(new AlluxioURI("/bar/y"), res4.getUri());
    Assert.assertEquals(3L, res4.getMountId());
    MountTable.Resolution res5 = mMountTable.resolve(new AlluxioURI("/mnt/bar/baz"));
    Assert.assertEquals(new AlluxioURI("/bar/baz"), res5.getUri());
    Assert.assertEquals(3L, res5.getMountId());
    MountTable.Resolution res6 = mMountTable.resolve(new AlluxioURI("/foobar"));
    Assert.assertEquals(new AlluxioURI(ROOT_UFS).join("foobar"), res6.getUri());
    Assert.assertEquals(IdUtils.ROOT_MOUNT_ID, res6.getMountId());
    MountTable.Resolution res7 = mMountTable.resolve(new AlluxioURI("/"));
    Assert.assertEquals(new AlluxioURI("s3a://bucket/"), res7.getUri());
    Assert.assertEquals(IdUtils.ROOT_MOUNT_ID, res7.getMountId());

    // Test reverseResolve()
    Assert.assertEquals(new AlluxioURI("/mnt/foo"),
        mMountTable.reverseResolve(new AlluxioURI("/foo")).getUri());
    Assert.assertEquals(new AlluxioURI("/mnt/foo/x"),
        mMountTable.reverseResolve(new AlluxioURI("/foo/x")).getUri());
    Assert.assertEquals(mMountTable.reverseResolve(new AlluxioURI("/bar")).getUri(),
        new AlluxioURI("/mnt/bar"));
    Assert.assertEquals(mMountTable.reverseResolve(new AlluxioURI("/bar/y")).getUri(),
        new AlluxioURI("/mnt/bar/y"));
    // Test reverseResolve(), ufs path is not mounted
    Assert.assertEquals(new AlluxioURI("/foobar"),
        mMountTable.reverseResolve(new AlluxioURI("s3a://bucket/foobar")).getUri());
    Assert.assertEquals(new AlluxioURI("/"),
        mMountTable.reverseResolve(new AlluxioURI("s3a://bucket/")).getUri());
    Assert.assertNull(mMountTable.reverseResolve(new AlluxioURI("/foobar")));

    // Test getMountPoint()
    Assert.assertEquals("/mnt/foo", mMountTable.getMountPoint(new AlluxioURI("/mnt/foo")));
    Assert.assertEquals("/mnt/foo", mMountTable.getMountPoint(new AlluxioURI("/mnt/foo/x")));
    Assert.assertEquals("/mnt/bar", mMountTable.getMountPoint(new AlluxioURI("/mnt/bar")));
    Assert.assertEquals("/mnt/bar", mMountTable.getMountPoint(new AlluxioURI("/mnt/bar/y")));
    Assert.assertEquals("/mnt/bar", mMountTable.getMountPoint(new AlluxioURI("/mnt/bar/baz")));
    Assert.assertEquals("/", mMountTable.getMountPoint(new AlluxioURI("/mnt")));
    Assert.assertEquals("/", mMountTable.getMountPoint(new AlluxioURI("/")));

    // Test isMountPoint()
    Assert.assertTrue(mMountTable.isMountPoint(new AlluxioURI("/")));
    Assert.assertTrue(mMountTable.isMountPoint(new AlluxioURI("/mnt/foo")));
    Assert.assertFalse(mMountTable.isMountPoint(new AlluxioURI("/mnt/foo/bar")));
    Assert.assertFalse(mMountTable.isMountPoint(new AlluxioURI("/mnt")));
    Assert.assertFalse(mMountTable.isMountPoint(new AlluxioURI("/mnt/foo3")));
    Assert.assertTrue(mMountTable.isMountPoint(new AlluxioURI("/mnt/bar")));
    Assert.assertFalse(mMountTable.isMountPoint(new AlluxioURI("/mnt/bar/baz")));

    // Test delete()
    Assert.assertTrue(deleteMount("/mnt/bar"));
    Assert.assertTrue(deleteMount("/mnt/foo"));
    Assert.assertFalse(deleteMount("/mnt/foo"));
    Assert.assertFalse(deleteMount("/"));

    try {
      addMount("alluxio://localhost/t1", "s3a://localhost", 5);
      addMount("alluxio://localhost/t2", "s3a://localhost", 5);
      Assert.fail("mount fails");
    } catch (InvalidPathException e) {
      // Exception expected
      Assert.assertEquals(ExceptionMessage.MOUNT_POINT_PREFIX_OF_ANOTHER
          .getMessage("s3a://localhost", "s3a://localhost"), e.getMessage());
    }
  }

  /**
   * Tests nested mount point.
   */
  @Test
  public void pathNestedMount() throws Exception {
    // Test add()
    addMount("/mnt/foo", "/foo", 2);
    addMount("/mnt/bar", "/bar", 3);
    // Testing nested mount
    addMount("/mnt/bar/baz", "/baz", 4);
    addMount("/mnt/bar/baz/bay", "/bay", 5);

    // Test resolve()
    MountTable.Resolution res1 = mMountTable.resolve(new AlluxioURI("/mnt/foo"));
    Assert.assertEquals(new AlluxioURI("/foo"), res1.getUri());
    Assert.assertEquals(2L, res1.getMountId());
    MountTable.Resolution res2 = mMountTable.resolve(new AlluxioURI("/mnt/foo/x"));
    Assert.assertEquals(new AlluxioURI("/foo/x"), res2.getUri());
    Assert.assertEquals(2L, res2.getMountId());
    MountTable.Resolution res3 = mMountTable.resolve(new AlluxioURI("/mnt/bar"));
    Assert.assertEquals(new AlluxioURI("/bar"), res3.getUri());
    Assert.assertEquals(3L, res3.getMountId());
    MountTable.Resolution res4 = mMountTable.resolve(new AlluxioURI("/mnt/bar/y"));
    Assert.assertEquals(new AlluxioURI("/bar/y"), res4.getUri());
    Assert.assertEquals(3L, res4.getMountId());
    MountTable.Resolution res5 = mMountTable.resolve(new AlluxioURI("/mnt/bar/baz"));
    Assert.assertEquals(new AlluxioURI("/baz"), res5.getUri());
    Assert.assertEquals(4L, res5.getMountId());
    MountTable.Resolution res6 = mMountTable.resolve(new AlluxioURI("/foobar"));
    Assert.assertEquals(new AlluxioURI(ROOT_UFS).join("foobar"), res6.getUri());
    Assert.assertEquals(IdUtils.ROOT_MOUNT_ID, res6.getMountId());
    MountTable.Resolution res7 = mMountTable.resolve(new AlluxioURI("/"));
    Assert.assertEquals(new AlluxioURI("s3a://bucket/"), res7.getUri());
    Assert.assertEquals(IdUtils.ROOT_MOUNT_ID, res7.getMountId());

    // Test getMountPoint()
    Assert.assertEquals("/mnt/foo", mMountTable.getMountPoint(new AlluxioURI("/mnt/foo")));
    Assert.assertEquals("/mnt/foo", mMountTable.getMountPoint(new AlluxioURI("/mnt/foo/x")));
    Assert.assertEquals("/mnt/bar", mMountTable.getMountPoint(new AlluxioURI("/mnt/bar")));
    Assert.assertEquals("/mnt/bar", mMountTable.getMountPoint(new AlluxioURI("/mnt/bar/y")));
    Assert.assertEquals("/mnt/bar/baz", mMountTable.getMountPoint(new AlluxioURI("/mnt/bar/baz")));
    Assert.assertEquals("/", mMountTable.getMountPoint(new AlluxioURI("/mnt")));
    Assert.assertEquals("/", mMountTable.getMountPoint(new AlluxioURI("/")));

    // Test isMountPoint()
    Assert.assertTrue(mMountTable.isMountPoint(new AlluxioURI("/")));
    Assert.assertTrue(mMountTable.isMountPoint(new AlluxioURI("/mnt/foo")));
    Assert.assertFalse(mMountTable.isMountPoint(new AlluxioURI("/mnt/foo/bar")));
    Assert.assertFalse(mMountTable.isMountPoint(new AlluxioURI("/mnt")));
    Assert.assertFalse(mMountTable.isMountPoint(new AlluxioURI("/mnt/foo3")));
    Assert.assertTrue(mMountTable.isMountPoint(new AlluxioURI("/mnt/bar")));
    Assert.assertTrue(mMountTable.isMountPoint(new AlluxioURI("/mnt/bar/baz")));

    // Test containsMountPoint()
    Assert.assertTrue(mMountTable.containsMountPoint(new AlluxioURI("/mnt/bar"), false));
    Assert.assertTrue(mMountTable.containsMountPoint(new AlluxioURI("/mnt/bar/baz"), false));
    Assert.assertFalse(mMountTable.containsMountPoint(new AlluxioURI("/mnt/bar/baz/bay"), false));
    Assert.assertFalse(mMountTable.containsMountPoint(new AlluxioURI("/mnt/foo"), false));
    Assert.assertTrue(mMountTable.containsMountPoint(new AlluxioURI("/mnt/foo"), true));
    Assert.assertTrue(mMountTable.containsMountPoint(new AlluxioURI("/"), true));
    Assert.assertFalse(mMountTable.containsMountPoint(new AlluxioURI("/bogus"), true));

    // Test delete()
    Assert.assertFalse(deleteMount("/mnt/bar/baz"));
    Assert.assertTrue(deleteMount("/mnt/bar/baz/bay"));
    Assert.assertTrue(deleteMount("/mnt/bar/baz"));
    Assert.assertTrue(deleteMount("/mnt/bar"));
    Assert.assertTrue(deleteMount("/mnt/foo"));
    Assert.assertFalse(deleteMount("/mnt/foo"));
    Assert.assertFalse(deleteMount("/"));
  }
  /**
   * Tests the different methods of the {@link MountTable} class with a URI.
   */
  @Test
  public void uri() throws Exception {
    // Test add()
    addMount("alluxio://localhost:1234/mnt/foo", "file://localhost:5678/foo", 2);
    addMount("alluxio://localhost:1234/mnt/bar", "file://localhost:5678/bar", 3);

    try {
      addMount("alluxio://localhost:1234/mnt/foo", "hdfs://localhost:5678/foo2", 4);
      Assert.fail("Mount point added when it already exists");
    } catch (FileAlreadyExistsException e) {
      // Exception expected
      Assert.assertEquals(ExceptionMessage.MOUNT_POINT_ALREADY_EXISTS.getMessage("/mnt/foo"),
          e.getMessage());
    }

    addMount("alluxio://localhost:1234/mnt/bar/baz", "hdfs://localhost:5678/baz", 5);

    // Test resolve()
    Assert.assertEquals(new AlluxioURI("file://localhost:5678/foo"),
        mMountTable.resolve(new AlluxioURI("alluxio://localhost:1234/mnt/foo")).getUri());
    Assert.assertEquals(new AlluxioURI("file://localhost:5678/bar"),
        mMountTable.resolve(new AlluxioURI("alluxio://localhost:1234/mnt/bar")).getUri());
    Assert.assertEquals(new AlluxioURI("file://localhost:5678/bar/y"),
        mMountTable.resolve(new AlluxioURI("alluxio://localhost:1234/mnt/bar/y")).getUri());
    Assert.assertEquals(new AlluxioURI("hdfs://localhost:5678/baz"),
        mMountTable.resolve(new AlluxioURI("alluxio://localhost:1234/mnt/bar/baz")).getUri());

    // Test getMountPoint()
    Assert.assertEquals("/mnt/foo",
        mMountTable.getMountPoint(new AlluxioURI("alluxio://localhost:1234/mnt/foo")));
    Assert.assertEquals("/mnt/bar",
        mMountTable.getMountPoint(new AlluxioURI("alluxio://localhost:1234/mnt/bar")));
    Assert.assertEquals("/mnt/bar",
        mMountTable.getMountPoint(new AlluxioURI("alluxio://localhost:1234/mnt/bar/y")));
    Assert.assertEquals("/mnt/bar/baz",
        mMountTable.getMountPoint(new AlluxioURI("alluxio://localhost:1234/mnt/bar/baz")));
    Assert.assertEquals("/",
        mMountTable.getMountPoint(new AlluxioURI("alluxio://localhost:1234/mnt")));
    Assert.assertEquals("/",
        mMountTable.getMountPoint(new AlluxioURI("alluxio://localhost:1234/")));

    // Test isMountPoint()
    Assert.assertTrue(mMountTable.isMountPoint(new AlluxioURI("alluxio://localhost:1234/")));
    Assert.assertTrue(mMountTable.isMountPoint(new AlluxioURI("alluxio://localhost:1234/mnt/foo")));
    Assert.assertFalse(
        mMountTable.isMountPoint(new AlluxioURI("alluxio://localhost:1234/mnt/foo/bar")));
    Assert.assertFalse(mMountTable.isMountPoint(new AlluxioURI("alluxio://localhost:1234/mnt")));
    Assert
        .assertFalse(mMountTable.isMountPoint(new AlluxioURI("alluxio://localhost:1234/mnt/foo2")));
    Assert
        .assertFalse(mMountTable.isMountPoint(new AlluxioURI("alluxio://localhost:1234/mnt/foo3")));
    Assert.assertTrue(mMountTable.isMountPoint(new AlluxioURI("alluxio://localhost:1234/mnt/bar")));
    Assert.assertTrue(
        mMountTable.isMountPoint(new AlluxioURI("alluxio://localhost:1234/mnt/bar/baz")));

    // Test delete()
    Assert.assertFalse(deleteMount("alluxio://localhost:1234/mnt/bar"));
    Assert.assertTrue(deleteMount("alluxio://localhost:1234/mnt/bar/baz"));
    Assert.assertTrue(deleteMount("alluxio://localhost:1234/mnt/bar"));
    Assert.assertTrue(deleteMount("alluxio://localhost:1234/mnt/foo"));
    Assert.assertFalse(deleteMount("alluxio://localhost:1234/mnt/foo"));
    Assert.assertFalse(deleteMount("alluxio://localhost:1234/"));
  }

  /**
   * Tests check of readonly mount points.
   */
  @Test
  public void readOnlyMount() throws Exception {
    MountPOptions options =
        MountContext.mergeFrom(MountPOptions.newBuilder().setReadOnly(true)).getOptions().build();
    String mountPath = "/mnt/foo";
    AlluxioURI alluxioUri = new AlluxioURI("alluxio://localhost:1234" + mountPath);
    mMountTable.add(NoopJournalContext.INSTANCE, alluxioUri,
        new AlluxioURI("hdfs://localhost:5678/foo"), 2L, options);

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
    addMount(alluxioUri.toString(), "hdfs://localhost:5678/foo", IdUtils.INVALID_MOUNT_ID);

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
        new MountInfo(new AlluxioURI("/mnt/foo"), new AlluxioURI("hdfs://localhost:5678/foo"), 2L,
            MountContext.defaults().getOptions().build()));
    mountTable.put("/mnt/bar",
        new MountInfo(new AlluxioURI("/mnt/bar"), new AlluxioURI("hdfs://localhost:5678/bar"), 3L,
            MountContext.defaults().getOptions().build()));

    AlluxioURI masterAddr = new AlluxioURI("alluxio://localhost:1234");
    for (Map.Entry<String, MountInfo> mountPoint : mountTable.entrySet()) {
      MountInfo mountInfo = mountPoint.getValue();
      mMountTable.add(NoopJournalContext.INSTANCE, masterAddr.join(mountPoint.getKey()),
          mountInfo.getUfsUri(), mountInfo.getMountId(), mountInfo.getOptions());
    }
    // Add root mountpoint
    mountTable.put("/", new MountInfo(new AlluxioURI("/"), new AlluxioURI("s3a://bucket/"),
        IdUtils.ROOT_MOUNT_ID, MountContext.defaults().getOptions().build()));
    Assert.assertEquals(mountTable, mMountTable.getMountTable());
  }

  /**
   * Tests the method for getting mount info given mount id.
   */
  @Test
  public void getMountInfo() throws Exception {
    MountInfo info1 =
        new MountInfo(new AlluxioURI("/mnt/foo"), new AlluxioURI("hdfs://localhost:5678/foo"), 2L,
            MountContext.defaults().getOptions().build());
    MountInfo info2 =
        new MountInfo(new AlluxioURI("/mnt/bar"), new AlluxioURI("hdfs://localhost:5678/bar"), 3L,
            MountContext.defaults().getOptions().build());
    addMount("/mnt/foo", "hdfs://localhost:5678/foo", 2);
    addMount("/mnt/bar", "hdfs://localhost:5678/bar", 3);
    Assert.assertEquals(info1, mMountTable.getMountInfo(info1.getMountId()));
    Assert.assertEquals(info2, mMountTable.getMountInfo(info2.getMountId()));
    Assert.assertEquals(null, mMountTable.getMountInfo(4L));
  }

  private void addMount(String alluxio, String ufs, long id) throws Exception {
    mMountTable.add(NoopJournalContext.INSTANCE, new AlluxioURI(alluxio), new AlluxioURI(ufs), id,
            MountContext.defaults().getOptions().build());
  }

  private boolean deleteMount(String path) {
    return mMountTable.delete(NoopJournalContext.INSTANCE, new AlluxioURI(path), true);
  }
}
