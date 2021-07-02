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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.ConfigurationTestUtils;
import alluxio.grpc.MountPOptions;
import alluxio.master.file.contexts.MountContext;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.journal.NoopJournalContext;
import alluxio.underfs.MasterUfsManager;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.IdUtils;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;

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

  @Rule
  public TemporaryFolder mTemp = new TemporaryFolder();

  /**
   * Sets up a new {@link AsyncUfsAbsentPathCache} before a test runs.
   */
  @Before
  public void before() throws Exception {
    mLocalUfsPath = mTemp.getRoot().getAbsolutePath();
    mUfsManager = new MasterUfsManager();
    mMountTable = new MountTable(mUfsManager, new MountInfo(new AlluxioURI("/"),
        new AlluxioURI("/ufs"), 1, MountContext.defaults().getOptions().build()));
    mUfsAbsentPathCache = new AsyncUfsAbsentPathCache(mMountTable, THREADS);

    mMountId = IdUtils.getRandomNonNegativeLong();
    MountPOptions options = MountContext.defaults().getOptions().build();
    mUfsManager.addMount(mMountId, new AlluxioURI(mLocalUfsPath),
        UnderFileSystemConfiguration.defaults(ConfigurationTestUtils.defaults())
            .setReadOnly(options.getReadOnly()).setShared(options.getShared())
            .createMountSpecificConf(Collections.<String, String>emptyMap()));
    mMountTable.add(NoopJournalContext.INSTANCE, new AlluxioURI("/mnt"),
        new AlluxioURI(mLocalUfsPath), mMountId, options);
  }

  @Test
  public void isAbsent() throws Exception {
    AlluxioURI absentPath = new AlluxioURI("/mnt/absent");
    // Existence of absentPath is not known yet
    assertFalse(mUfsAbsentPathCache.isAbsentSince(absentPath, UfsAbsentPathCache.ALWAYS));
    process(absentPath);
    // absentPath is known to be absent
    assertTrue(mUfsAbsentPathCache.isAbsentSince(absentPath, UfsAbsentPathCache.ALWAYS));
    // child of absentPath is also known to be absent
    assertTrue(mUfsAbsentPathCache.isAbsentSince(absentPath.join("a"), UfsAbsentPathCache.ALWAYS));

    mTemp.newFolder("folder");
    AlluxioURI newFolder = new AlluxioURI("/mnt/folder");
    // Existence of newFolder is not known yet
    assertFalse(mUfsAbsentPathCache.isAbsentSince(newFolder, UfsAbsentPathCache.ALWAYS));
    process(newFolder);
    // newFolder is known to exist
    assertFalse(mUfsAbsentPathCache.isAbsentSince(newFolder, UfsAbsentPathCache.ALWAYS));
    // Existence of child of newFolder is not known
    assertFalse(mUfsAbsentPathCache.isAbsentSince(newFolder.join("a"), UfsAbsentPathCache.ALWAYS));
  }

  @Test
  public void isAbsentRoot() throws Exception {
    // /mnt/a will be the first absent path
    process(new AlluxioURI("/mnt/a/b"));
    checkPaths(new AlluxioURI("/mnt/a"));

    // /mnt/a will be the first absent path
    process(new AlluxioURI("/mnt/a/b/c"));
    checkPaths(new AlluxioURI("/mnt/a"));

    // /mnt/1 will be the first absent path
    process(new AlluxioURI("/mnt/1/2"));
    checkPaths(new AlluxioURI("/mnt/1"));

    // /mnt/1 will be the first absent path
    process(new AlluxioURI("/mnt/1/3"));
    checkPaths(new AlluxioURI("/mnt/1"));
  }

  @Test
  public void isAbsentDirectory() throws Exception {
    String ufsBase = "/a/b";
    String alluxioBase = "/mnt" + ufsBase;
    // Create ufs directories
    assertTrue((new File(mLocalUfsPath + ufsBase)).mkdirs());

    // 'base + /c' will be the first absent path
    process(new AlluxioURI(alluxioBase + "/c/d"));
    checkPaths(new AlluxioURI(alluxioBase + "/c"));

    // 'base + /c' will be the first absent path
    process(new AlluxioURI(alluxioBase + "/c/d/e"));
    checkPaths(new AlluxioURI(alluxioBase + "/c"));

    // '/a/1' will be the first absent path
    process(new AlluxioURI("/mnt/a/1/2"));
    checkPaths(new AlluxioURI("/mnt/a/1"));

    // '/1' will be the first absent path
    process(new AlluxioURI("/mnt/1/2"));
    checkPaths(new AlluxioURI("/mnt/1"));
  }

  @Test
  public void isAbsentAddUfsDirectory() throws Exception {
    String ufsBase = "/a/b";
    String alluxioBase = "/mnt" + ufsBase;
    // Create ufs directories
    assertTrue((new File(mLocalUfsPath + ufsBase)).mkdirs());

    // 'base + /c' will be the first absent path
    process(new AlluxioURI(alluxioBase + "/c/d/e"));
    checkPaths(new AlluxioURI(alluxioBase + "/c"));

    // Create a sub-directory in ufs
    assertTrue((new File(mLocalUfsPath + ufsBase + "/c")).mkdirs());

    // Now, 'base + /c/d' will be the first absent path
    process(new AlluxioURI(alluxioBase + "/c/d/e"));
    checkPaths(new AlluxioURI(alluxioBase + "/c/d"));
  }

  @Test
  public void isAbsentRemoveUfsDirectory() throws Exception {
    String ufsBase = "/a/b";
    String alluxioBase = "/mnt" + ufsBase;
    // Create ufs directories
    assertTrue((new File(mLocalUfsPath + ufsBase)).mkdirs());

    // 'base + /c' will be the first absent path
    process(new AlluxioURI(alluxioBase + "/c/d/e"));
    checkPaths(new AlluxioURI(alluxioBase + "/c"));

    // delete '/a/b' from ufs
    assertTrue((new File(mLocalUfsPath + ufsBase)).delete());

    // Now, '/a/b' will be the first absent path
    process(new AlluxioURI(alluxioBase + "/c/d/e"));
    checkPaths(new AlluxioURI(alluxioBase));
  }

  @Test
  public void removeMountPoint() throws Exception {
    String ufsBase = "/a/b";
    String alluxioBase = "/mnt" + ufsBase;
    // Create ufs directories
    assertTrue((new File(mLocalUfsPath + ufsBase)).mkdirs());

    // 'base + /c' will be the first absent path
    process(new AlluxioURI(alluxioBase + "/c/d"));
    checkPaths(new AlluxioURI(alluxioBase + "/c"));

    // Unmount
    assertTrue(
        mMountTable.delete(NoopJournalContext.INSTANCE, new AlluxioURI("/mnt"), true));

    // Re-mount the same ufs
    long newMountId = IdUtils.getRandomNonNegativeLong();
    MountPOptions options = MountContext.defaults().getOptions().build();
    mUfsManager.addMount(newMountId, new AlluxioURI(mLocalUfsPath),
        UnderFileSystemConfiguration.defaults(ConfigurationTestUtils.defaults())
            .setReadOnly(options.getReadOnly()).setShared(options.getShared())
            .createMountSpecificConf(Collections.<String, String>emptyMap()));
    mMountTable.add(NoopJournalContext.INSTANCE, new AlluxioURI("/mnt"),
        new AlluxioURI(mLocalUfsPath), newMountId, options);

    // The cache should not contain any paths now.
    assertFalse(mUfsAbsentPathCache.isAbsentSince(new AlluxioURI("/mnt/a/b/c/d"),
        UfsAbsentPathCache.ALWAYS));
    assertFalse(mUfsAbsentPathCache.isAbsentSince(new AlluxioURI("/mnt/a/b/c"),
        UfsAbsentPathCache.ALWAYS));
    assertFalse(mUfsAbsentPathCache.isAbsentSince(new AlluxioURI("/mnt/a/b"),
        UfsAbsentPathCache.ALWAYS));
    assertFalse(mUfsAbsentPathCache.isAbsentSince(new AlluxioURI("/mnt/a"),
        UfsAbsentPathCache.ALWAYS));
    assertFalse(mUfsAbsentPathCache.isAbsentSince(new AlluxioURI("/mnt/"),
        UfsAbsentPathCache.ALWAYS));
  }

  @Test
  public void removePath() throws Exception {
    String ufsBase = "/a/b";
    String alluxioBase = "/mnt" + ufsBase;
    // Create ufs directories
    assertTrue((new File(mLocalUfsPath + ufsBase)).mkdirs());

    // 'base + /c' will be the first absent path
    process(new AlluxioURI(alluxioBase + "/c/d"));
    checkPaths(new AlluxioURI(alluxioBase + "/c"));

    // Create additional ufs directories
    assertTrue((new File(mLocalUfsPath + ufsBase + "/c/d")).mkdirs());
    process(new AlluxioURI(alluxioBase + "/c/d"));

    assertFalse(mUfsAbsentPathCache.isAbsentSince(new AlluxioURI("/mnt/a/b/c/d"),
        UfsAbsentPathCache.ALWAYS));
    assertFalse(mUfsAbsentPathCache.isAbsentSince(new AlluxioURI("/mnt/a/b/c"),
        UfsAbsentPathCache.ALWAYS));
    assertFalse(mUfsAbsentPathCache.isAbsentSince(new AlluxioURI("/mnt/a/b"),
        UfsAbsentPathCache.ALWAYS));
    assertFalse(mUfsAbsentPathCache.isAbsentSince(new AlluxioURI("/mnt/a"),
        UfsAbsentPathCache.ALWAYS));
    assertFalse(mUfsAbsentPathCache.isAbsentSince(new AlluxioURI("/mnt/"),
        UfsAbsentPathCache.ALWAYS));
  }

  private void process(AlluxioURI path) throws Exception {
    mUfsAbsentPathCache.processPathSync(path, Collections.emptyList());
  }

  /**
   * Checks for absent paths (descendants) and existing paths (ancestors) in the UFS.
   *
   * @param firstAbsent the first Alluxio path which should not exist in the UFS
   */
  private void checkPaths(AlluxioURI firstAbsent) throws Exception {
    // Check for additional non-existing paths as descendants of the first absent path
    for (int level = 1; level <= 2; level++) {
      AlluxioURI levelUri = firstAbsent.join("level" + level);
      for (int dir = 1; dir <= 2; dir++) {
        AlluxioURI uri = levelUri.join("dir" + dir);
        assertTrue(uri.toString(),
            mUfsAbsentPathCache.isAbsentSince(uri, UfsAbsentPathCache.ALWAYS));
      }
    }

    // Check all ancestors
    AlluxioURI existing = firstAbsent.getParent();
    while (existing != null) {
      assertFalse(existing.toString(), mUfsAbsentPathCache.isAbsentSince(existing,
          UfsAbsentPathCache.ALWAYS));
      existing = existing.getParent();
    }
  }
}
