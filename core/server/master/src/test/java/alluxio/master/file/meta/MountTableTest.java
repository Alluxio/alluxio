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
import alluxio.conf.Configuration;
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

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link MountTable}.
 */
public final class MountTableTest extends BaseInodeLockingTest {
  protected InodeLockManager mInodeLockManager = new InodeLockManager();
  private MountTable mMountTable;
  private static final String ROOT_UFS = "s3a://bucket/";
  private final UnderFileSystem mTestUfs = new LocalUnderFileSystemFactory().create("/",
      UnderFileSystemConfiguration.defaults(Configuration.global()));

  @Before
  public void before() throws Exception {
    UfsManager ufsManager = mock(UfsManager.class);
    UfsClient ufsClient =
        new UfsManager.UfsClient(() -> mTestUfs, AlluxioURI.EMPTY_URI);
    when(ufsManager.get(anyLong())).thenReturn(ufsClient);
    mMountTable = new MountTable(ufsManager,
        new MountInfo(new AlluxioURI(MountTable.ROOT), new AlluxioURI(ROOT_UFS),
            IdUtils.ROOT_MOUNT_ID, MountContext.defaults().getOptions().build()),
        Clock.systemUTC());
    mMountTable.enableMountTableTrie(mRootDir);
  }

  /**
   * Tests the different methods of the {@link MountTable} class with a path.
   */
  @Test
  public void path() throws Exception {
    Assert.assertEquals(IdUtils.ROOT_MOUNT_ID,
        mMountTable.getMountInfo(new AlluxioURI(MountTable.ROOT)).getMountId());

    // Test add()
    LockedInodePath path1 = addMount("/mnt/foo", "/foo", 2);
    LockedInodePath path2 = addMount("/mnt/bar", "/bar", 3);
    LockedInodePath tmpInodePath2 =
        createLockedInodePath("/mnt/foo/x");
    LockedInodePath tmpInodePath4 =
        createLockedInodePath("/mnt/bar/y");
    LockedInodePath tmpInodePath5 =
        createLockedInodePath("/mnt/bar/baz");
    LockedInodePath tmpInodePath6 =
        createLockedInodePath("/foobar");
    LockedInodePath tmpInodePath7 =
        createLockedInodePath("/");
    LockedInodePath tmpInodePath8 =
        createLockedInodePath("/mnt");

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
    MountTable.Resolution res1 = mMountTable.resolve(path1);
    Assert.assertEquals(new AlluxioURI("/foo"), res1.getUri());
    Assert.assertEquals(2L, res1.getMountId());

    MountTable.Resolution res2 = mMountTable.resolve(tmpInodePath2);
    Assert.assertEquals(new AlluxioURI("/foo/x"), res2.getUri());
    Assert.assertEquals(2L, res2.getMountId());

    MountTable.Resolution res3 = mMountTable.resolve(path2);
    Assert.assertEquals(new AlluxioURI("/bar"), res3.getUri());
    Assert.assertEquals(3L, res3.getMountId());

    MountTable.Resolution res4 = mMountTable.resolve(tmpInodePath4);
    Assert.assertEquals(new AlluxioURI("/bar/y"), res4.getUri());
    Assert.assertEquals(3L, res4.getMountId());

    MountTable.Resolution res5 = mMountTable.resolve(tmpInodePath5);
    Assert.assertEquals(new AlluxioURI("/bar/baz"), res5.getUri());
    Assert.assertEquals(3L, res5.getMountId());

    MountTable.Resolution res6 = mMountTable.resolve(tmpInodePath6);
    Assert.assertEquals(new AlluxioURI(ROOT_UFS).join("foobar"), res6.getUri());
    Assert.assertEquals(IdUtils.ROOT_MOUNT_ID, res6.getMountId());

    MountTable.Resolution res7 = mMountTable.resolve(tmpInodePath7);
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
    Assert.assertEquals("/mnt/foo", mMountTable.getMountPoint(path1));
    Assert.assertEquals("/mnt/foo", mMountTable.getMountPoint(tmpInodePath2));
    Assert.assertEquals("/mnt/bar", mMountTable.getMountPoint(path2));
    Assert.assertEquals("/mnt/bar", mMountTable.getMountPoint(tmpInodePath4));
    Assert.assertEquals("/mnt/bar", mMountTable.getMountPoint(tmpInodePath5));
    Assert.assertEquals("/", mMountTable.getMountPoint(tmpInodePath8));
    Assert.assertEquals("/", mMountTable.getMountPoint(tmpInodePath7));

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
    LockedInodePath path1 = addMount("/mnt/foo", "/foo", 2);
    LockedInodePath path2 = addMount("/mnt/bar", "/bar", 3);
    // Testing nested mount
    LockedInodePath path3 = addMount("/mnt/bar/baz", "/baz", 4);
    LockedInodePath path4 = addMount("/mnt/bar/baz/bay", "/bay", 5);
    LockedInodePath tmpInodePath1 =
        createLockedInodePath("/mnt/foo/x");
    LockedInodePath tmpInodePath2 =
        createLockedInodePath("/mnt/bar/y");
    LockedInodePath tmpInodePath3 =
        createLockedInodePath("/mnt/bar/baz");
    LockedInodePath tmpInodePath4 =
        createLockedInodePath("/foobar");
    LockedInodePath tmpInodePath5 =
        createLockedInodePath("/");
    LockedInodePath tmpInodePath6 =
        createLockedInodePath("/mnt");
    LockedInodePath tmpInodePath7 =
        createLockedInodePath("/bogus");

    // Test resolve()
    MountTable.Resolution res1 = mMountTable.resolve(path1);
    Assert.assertEquals(new AlluxioURI("/foo"), res1.getUri());
    Assert.assertEquals(2L, res1.getMountId());
    MountTable.Resolution res2 = mMountTable.resolve(tmpInodePath1);
    Assert.assertEquals(new AlluxioURI("/foo/x"), res2.getUri());
    Assert.assertEquals(2L, res2.getMountId());
    MountTable.Resolution res3 = mMountTable.resolve(path2);
    Assert.assertEquals(new AlluxioURI("/bar"), res3.getUri());
    Assert.assertEquals(3L, res3.getMountId());
    MountTable.Resolution res4 = mMountTable.resolve(tmpInodePath2);
    Assert.assertEquals(new AlluxioURI("/bar/y"), res4.getUri());
    Assert.assertEquals(3L, res4.getMountId());
    MountTable.Resolution res5 = mMountTable.resolve(tmpInodePath3);
    Assert.assertEquals(new AlluxioURI("/baz"), res5.getUri());
    Assert.assertEquals(4L, res5.getMountId());
    MountTable.Resolution res6 = mMountTable.resolve(tmpInodePath4);
    Assert.assertEquals(new AlluxioURI(ROOT_UFS).join("foobar"), res6.getUri());
    Assert.assertEquals(IdUtils.ROOT_MOUNT_ID, res6.getMountId());
    MountTable.Resolution res7 = mMountTable.resolve(tmpInodePath5);
    Assert.assertEquals(new AlluxioURI("s3a://bucket/"), res7.getUri());
    Assert.assertEquals(IdUtils.ROOT_MOUNT_ID, res7.getMountId());

    // Test getMountPoint()
    Assert.assertEquals("/mnt/foo", mMountTable.getMountPoint(path1));
    Assert.assertEquals("/mnt/foo", mMountTable.getMountPoint(tmpInodePath1));
    Assert.assertEquals("/mnt/bar", mMountTable.getMountPoint(path2));
    Assert.assertEquals("/mnt/bar", mMountTable.getMountPoint(tmpInodePath2));
    Assert.assertEquals("/mnt/bar/baz", mMountTable.getMountPoint(tmpInodePath3));
    Assert.assertEquals("/", mMountTable.getMountPoint(tmpInodePath6));
    Assert.assertEquals("/", mMountTable.getMountPoint(tmpInodePath5));

    // Test isMountPoint()
    Assert.assertTrue(mMountTable.isMountPoint(new AlluxioURI("/")));
    Assert.assertTrue(mMountTable.isMountPoint(new AlluxioURI("/mnt/foo")));
    Assert.assertFalse(mMountTable.isMountPoint(new AlluxioURI("/mnt/foo/bar")));
    Assert.assertFalse(mMountTable.isMountPoint(new AlluxioURI("/mnt")));
    Assert.assertFalse(mMountTable.isMountPoint(new AlluxioURI("/mnt/foo3")));
    Assert.assertTrue(mMountTable.isMountPoint(new AlluxioURI("/mnt/bar")));
    Assert.assertTrue(mMountTable.isMountPoint(new AlluxioURI("/mnt/bar/baz")));

    // Test containsMountPoint()
    Assert.assertTrue(mMountTable.containsMountPoint(path2, false));
    Assert.assertTrue(mMountTable.containsMountPoint(path3, false));
    Assert.assertFalse(mMountTable.containsMountPoint(path4, false));
    Assert.assertFalse(mMountTable.containsMountPoint(path1, false));
    Assert.assertTrue(mMountTable.containsMountPoint(path1, true));
    Assert.assertTrue(mMountTable.containsMountPoint(tmpInodePath5, true));
    Assert.assertFalse(mMountTable.containsMountPoint(tmpInodePath7, true));

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
    LockedInodePath path1 = addMount("alluxio://localhost:1234/mnt/foo",
        "file://localhost:5678/foo", 2);
    LockedInodePath path2 = addMount("alluxio://localhost:1234/mnt/bar",
        "file://localhost:5678" + "/bar", 3);

    try {
      addMount("alluxio://localhost:1234/mnt/foo", "hdfs://localhost:5678/foo2", 4);
      Assert.fail("Mount point added when it already exists");
    } catch (FileAlreadyExistsException e) {
      // Exception expected
      Assert.assertEquals(
          ExceptionMessage.MOUNT_POINT_ALREADY_EXISTS.getMessage("/mnt/foo"),
          e.getMessage());
    }

    LockedInodePath path3 = addMount("alluxio://localhost:1234/mnt/bar/baz",
        "hdfs://localhost" + ":5678/baz", 5);
    LockedInodePath tmpLockedPath1 = createLockedInodePath(
        "alluxio://localhost:1234/mnt/bar/y");
    LockedInodePath tmpLockedPath2 = createLockedInodePath("alluxio://localhost:1234/mnt"
    );
    LockedInodePath tmpLockedPath3 = createLockedInodePath("alluxio://localhost:1234/"
    );

    // Test resolve()
    Assert.assertEquals(new AlluxioURI("file://localhost:5678/foo"),
        mMountTable.resolve(path1).getUri());
    Assert.assertEquals(new AlluxioURI("file://localhost:5678/bar"),
        mMountTable.resolve(path2).getUri());
    Assert.assertEquals(new AlluxioURI("file://localhost:5678/bar/y"),
        mMountTable.resolve(tmpLockedPath1).getUri());
    Assert.assertEquals(new AlluxioURI("hdfs://localhost:5678/baz"),
        mMountTable.resolve(path3).getUri());

    // Test getMountPoint()
    Assert.assertEquals("/mnt/foo",
        mMountTable.getMountPoint(path1));
    Assert.assertEquals("/mnt/bar",
        mMountTable.getMountPoint(path2));
    Assert.assertEquals("/mnt/bar",
        mMountTable.getMountPoint(tmpLockedPath1));
    Assert.assertEquals("/mnt/bar/baz",
        mMountTable.getMountPoint(path3));
    Assert.assertEquals("/",
        mMountTable.getMountPoint(tmpLockedPath2));
    Assert.assertEquals("/",
        mMountTable.getMountPoint(tmpLockedPath3));

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
    LockedInodePath alluxioLockedInodePath = createLockedInodePath(alluxioUri.getPath()
    );
    mMountTable.add(NoopJournalContext.INSTANCE, alluxioLockedInodePath,
        new AlluxioURI("hdfs://localhost:5678/foo"), 2L, options);

    try {
      mMountTable.checkUnderWritableMountPoint(alluxioLockedInodePath);
      Assert.fail("Readonly mount point should not be writable.");
    } catch (AccessControlException e) {
      // Exception expected
      Assert.assertEquals(ExceptionMessage.MOUNT_READONLY.getMessage(mountPath, mountPath),
          e.getMessage());
    }

    try {
      String path = mountPath + "/sub/f1";
      alluxioUri = new AlluxioURI("alluxio://localhost:1234" + path);
      LockedInodePath inodePath = createLockedInodePath(alluxioUri.getPath()
      );
      mMountTable.checkUnderWritableMountPoint(inodePath);
      Assert.fail("Readonly mount point should not be writable.");
    } catch (AccessControlException e) {
      // Exception expected
      Assert.assertEquals(
          ExceptionMessage.MOUNT_READONLY.getMessage(mountPath + "/sub/f1", mountPath),
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
    LockedInodePath path1 = addMount(alluxioUri.toString(), "hdfs://localhost:5678/foo",
        IdUtils.INVALID_MOUNT_ID);

    try {
      mMountTable.checkUnderWritableMountPoint(path1);
    } catch (AccessControlException e) {
      Assert.fail("Default mount point should be writable.");
    }

    try {
      String path = mountPath + "/sub/directory";
      LockedInodePath path2 = createLockedInodePath("alluxio://localhost:1234" + path
      );
      mMountTable.checkUnderWritableMountPoint(path2);
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
      LockedInodePath path = createLockedInodePath(masterAddr.join(mountPoint.getKey()).getPath()
      );
      mMountTable.add(NoopJournalContext.INSTANCE, path,
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
    Assert.assertNull(mMountTable.getMountInfo(4L));
  }

  private LockedInodePath addMount(String alluxio, String ufs, long id) throws Exception {
    LockedInodePath inodePath = createLockedInodePath(alluxio);
    mMountTable.add(NoopJournalContext.INSTANCE, inodePath, new AlluxioURI(ufs), id,
            MountContext.defaults().getOptions().build());
    return inodePath;
  }

  private boolean deleteMount(String path) throws Exception {
    LockedInodePath inodePath = createLockedInodePath(path);
    return mMountTable.delete(NoopJournalContext.INSTANCE, inodePath, true);
  }

  private LockedInodePath createLockedInodePath(String path)
      throws InvalidPathException {
    LockedInodePath lockedPath = new LockedInodePath(new AlluxioURI(path), mInodeStore,
        mInodeLockManager, mRootDir, InodeTree.LockPattern.READ, false, NoopJournalContext.INSTANCE);
    lockedPath.traverse();
    return lockedPath;
  }
}
