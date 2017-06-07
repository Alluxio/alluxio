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
import alluxio.master.file.options.MountOptions;
import alluxio.underfs.MasterUfsManager;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.CommonUtils;
import alluxio.util.IdUtils;
import alluxio.util.WaitForOptions;

import com.google.common.base.Function;
import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.io.File;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Unit tests for {@link AsyncUfsAbsentPathCache}.
 */
public class AsyncUfsAbsentPathCacheTest {
  private static final int THREADS = 4;
  private AsyncUfsAbsentPathCache mUfsAbsentPathCache;
  private MountTable mMountTable;

  private long mMountId;
  private UfsManager mUfsManager;
  private String mLocalUfsPath;

  /**
   * Sets up a new {@link AsyncUfsAbsentPathCache} before a test runs.
   */
  @Before
  public void before() throws Exception {
    mLocalUfsPath = Files.createTempDir().getAbsolutePath();
    mUfsManager = new MasterUfsManager();
    mMountTable = new MountTable(mUfsManager);
    mUfsAbsentPathCache = new AsyncUfsAbsentPathCache(mMountTable, THREADS);

    mMountId = IdUtils.getRandomNonNegativeLong();
    MountOptions options = MountOptions.defaults();
    mUfsManager.addMount(mMountId, new AlluxioURI(mLocalUfsPath),
        UnderFileSystemConfiguration.defaults().setReadOnly(options.isReadOnly())
            .setShared(options.isShared())
            .setUserSpecifiedConf(Collections.<String, String>emptyMap()));
    mMountTable.add(new AlluxioURI("/mnt"), new AlluxioURI(mLocalUfsPath), mMountId, options);
  }

  @Test
  public void isAbsentRoot() throws Exception {
    // /mnt/a will be the first absent path
    addAbsent(new AlluxioURI("/mnt/a/b"));
    checkAbsentPaths(new AlluxioURI("/mnt/a"));

    // /mnt/a will be the first absent path
    addAbsent(new AlluxioURI("/mnt/a/b/c"));
    checkAbsentPaths(new AlluxioURI("/mnt/a"));

    // /mnt/1 will be the first absent path
    addAbsent(new AlluxioURI("/mnt/1/2"));
    checkAbsentPaths(new AlluxioURI("/mnt/1"));

    // /mnt/1 will be the first absent path
    addAbsent(new AlluxioURI("/mnt/1/3"));
    checkAbsentPaths(new AlluxioURI("/mnt/1"));
  }

  @Test
  public void isAbsentDirectory() throws Exception {
    String ufsBase = "/a/b";
    String alluxioBase = "/mnt" + ufsBase;
    // Create ufs directories
    Assert.assertTrue((new File(mLocalUfsPath + ufsBase)).mkdirs());

    // 'base + /c' will be the first absent path
    addAbsent(new AlluxioURI(alluxioBase + "/c/d"));
    checkAbsentPaths(new AlluxioURI(alluxioBase + "/c"));

    // 'base + /c' will be the first absent path
    addAbsent(new AlluxioURI(alluxioBase + "/c/d/e"));
    checkAbsentPaths(new AlluxioURI(alluxioBase + "/c"));

    // '/a/1' will be the first absent path
    addAbsent(new AlluxioURI("/mnt/a/1/2"));
    checkAbsentPaths(new AlluxioURI("/mnt/a/1"));

    // '/1' will be the first absent path
    addAbsent(new AlluxioURI("/mnt/1/2"));
    checkAbsentPaths(new AlluxioURI("/mnt/1"));
  }

  @Test
  public void isAbsentAddUfsDirectory() throws Exception {
    String ufsBase = "/a/b";
    String alluxioBase = "/mnt" + ufsBase;
    // Create ufs directories
    Assert.assertTrue((new File(mLocalUfsPath + ufsBase)).mkdirs());

    // 'base + /c' will be the first absent path
    addAbsent(new AlluxioURI(alluxioBase + "/c/d/e"));
    checkAbsentPaths(new AlluxioURI(alluxioBase + "/c"));

    // Create a sub-directory in ufs
    Assert.assertTrue((new File(mLocalUfsPath + ufsBase + "/c")).mkdirs());

    // Now, 'base + /c/d' will be the first absent path
    addAbsent(new AlluxioURI(alluxioBase + "/c/d/e"));
    checkAbsentPaths(new AlluxioURI(alluxioBase + "/c/d"));
  }

  @Test
  public void isAbsentRemoveUfsDirectory() throws Exception {
    String ufsBase = "/a/b";
    String alluxioBase = "/mnt" + ufsBase;
    // Create ufs directories
    Assert.assertTrue((new File(mLocalUfsPath + ufsBase)).mkdirs());

    // 'base + /c' will be the first absent path
    addAbsent(new AlluxioURI(alluxioBase + "/c/d/e"));
    checkAbsentPaths(new AlluxioURI(alluxioBase + "/c"));

    // delete '/a/b' from ufs
    Assert.assertTrue((new File(mLocalUfsPath + ufsBase)).delete());

    // Now, '/a/b' will be the first absent path
    addAbsent(new AlluxioURI(alluxioBase + "/c/d/e"));
    checkAbsentPaths(new AlluxioURI(alluxioBase));
  }

  @Test
  public void removeMountPoint() throws Exception {
    String ufsBase = "/a/b";
    String alluxioBase = "/mnt" + ufsBase;
    // Create ufs directories
    Assert.assertTrue((new File(mLocalUfsPath + ufsBase)).mkdirs());

    // 'base + /c' will be the first absent path
    addAbsent(new AlluxioURI(alluxioBase + "/c/d"));
    checkAbsentPaths(new AlluxioURI(alluxioBase + "/c"));

    // Unmount
    Assert.assertTrue(mMountTable.delete(new AlluxioURI("/mnt")));

    // Re-mount the same ufs
    long newMountId = IdUtils.getRandomNonNegativeLong();
    MountOptions options = MountOptions.defaults();
    mUfsManager.addMount(newMountId, new AlluxioURI(mLocalUfsPath),
        UnderFileSystemConfiguration.defaults().setReadOnly(options.isReadOnly())
            .setShared(options.isShared())
            .setUserSpecifiedConf(Collections.<String, String>emptyMap()));
    mMountTable.add(new AlluxioURI("/mnt"), new AlluxioURI(mLocalUfsPath), newMountId, options);

    // The cache should not contain any paths now.
    Assert.assertFalse(mUfsAbsentPathCache.isAbsent(new AlluxioURI("/mnt/a/b/c/d")));
    Assert.assertFalse(mUfsAbsentPathCache.isAbsent(new AlluxioURI("/mnt/a/b/c")));
    Assert.assertFalse(mUfsAbsentPathCache.isAbsent(new AlluxioURI("/mnt/a/b")));
    Assert.assertFalse(mUfsAbsentPathCache.isAbsent(new AlluxioURI("/mnt/a")));
    Assert.assertFalse(mUfsAbsentPathCache.isAbsent(new AlluxioURI("/mnt/")));
  }

  @Test
  public void removePath() throws Exception {
    String ufsBase = "/a/b";
    String alluxioBase = "/mnt" + ufsBase;
    // Create ufs directories
    Assert.assertTrue((new File(mLocalUfsPath + ufsBase)).mkdirs());

    // 'base + /c' will be the first absent path
    addAbsent(new AlluxioURI(alluxioBase + "/c/d"));
    checkAbsentPaths(new AlluxioURI(alluxioBase + "/c"));

    // Create additional ufs directories
    Assert.assertTrue((new File(mLocalUfsPath + ufsBase + "/c/d")).mkdirs());
    removeAbsent(new AlluxioURI(alluxioBase + "/c/d"));

    Assert.assertFalse(mUfsAbsentPathCache.isAbsent(new AlluxioURI("/mnt/a/b/c/d")));
    Assert.assertFalse(mUfsAbsentPathCache.isAbsent(new AlluxioURI("/mnt/a/b/c")));
    Assert.assertFalse(mUfsAbsentPathCache.isAbsent(new AlluxioURI("/mnt/a/b")));
    Assert.assertFalse(mUfsAbsentPathCache.isAbsent(new AlluxioURI("/mnt/a")));
    Assert.assertFalse(mUfsAbsentPathCache.isAbsent(new AlluxioURI("/mnt/")));
  }

  private void addAbsent(AlluxioURI path) throws Exception {
    final ThreadPoolExecutor pool = Whitebox.getInternalState(mUfsAbsentPathCache, "mPool");
    final long initialTasks = pool.getCompletedTaskCount();
    mUfsAbsentPathCache.process(path);
    // Wait until the async task is completed.
    CommonUtils
        .waitFor("path (" + path + ") to be added to absent cache", new Function<Void, Boolean>() {
          @Override
          public Boolean apply(Void input) {
            if (pool.getCompletedTaskCount() == initialTasks) {
              return false;
            }
            return true;
          }
        }, WaitForOptions.defaults().setTimeoutMs(10000));
  }

  private void removeAbsent(AlluxioURI path) throws Exception {
    final ThreadPoolExecutor pool = Whitebox.getInternalState(mUfsAbsentPathCache, "mPool");
    final long initialTasks = pool.getCompletedTaskCount();
    mUfsAbsentPathCache.process(path);
    // Wait until the async task is completed.
    CommonUtils.waitFor("path (" + path + ") to be removed from absent cache",
        new Function<Void, Boolean>() {
          @Override
          public Boolean apply(Void input) {
            if (pool.getCompletedTaskCount() == initialTasks) {
              return false;
            }
            return true;
          }
        }, WaitForOptions.defaults().setTimeoutMs(10000));
  }

  /**
   * Checks for absent paths (descendants) and existing paths (ancestors) in the UFS.
   *
   * @param firstAbsent the first Alluxio path which should not exist in the UFS
   */
  private void checkAbsentPaths(AlluxioURI firstAbsent) throws Exception {
    // Check for additional non-existing paths as descendants of the first absent path
    for (int level = 1; level <= 2; level++) {
      AlluxioURI levelUri = firstAbsent.join("level" + level);
      for (int dir = 1; dir <= 2; dir++) {
        AlluxioURI uri = levelUri.join("dir" + dir);
        Assert.assertTrue(uri.toString(), mUfsAbsentPathCache.isAbsent(uri));
      }
    }

    // Check all ancestors
    AlluxioURI existing = firstAbsent.getParent();
    while (existing != null) {
      Assert.assertFalse(existing.toString(), mUfsAbsentPathCache.isAbsent(existing));
      existing = existing.getParent();
    }
  }
}
