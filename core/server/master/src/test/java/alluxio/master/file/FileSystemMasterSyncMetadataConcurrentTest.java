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

package alluxio.master.file;

import static org.junit.Assert.assertEquals;

import alluxio.AlluxioURI;
import alluxio.collections.Pair;
import alluxio.concurrent.jsr.CompletableFuture;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.InvalidPathException;
import alluxio.file.options.DescendantType;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockingScheme;
import alluxio.underfs.UnderFileSystem;

import com.google.common.math.IntMath;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

@RunWith(PowerMockRunner.class)
@PrepareForTest({UnderFileSystem.Factory.class})
public class FileSystemMasterSyncMetadataConcurrentTest
    extends FileSystemMasterSyncMetadataTestBase {
  private final int mNumDirsPerLevel = 2;
  private final int mNumLevels = 3;
  private final int mNumExpectedInodes = IntMath.pow(mNumDirsPerLevel, mNumLevels + 1) - 1;

  @Override
  public void before() throws Exception {
    super.before();
    Configuration.set(PropertyKey.MASTER_METADATA_CONCURRENT_SYNC_DEDUP, true);

    createUfsHierarchy(0, mNumLevels, "", mNumDirsPerLevel);

    mUfs.mIsSlow = true;
    mUfs.mSlowTimeMs = 500;
    // verify the files don't exist in alluxio
    assertEquals(1, mFileSystemMaster.getInodeTree().getInodeCount());
  }

  @Test
  public void loadMetadataForTheSameDirectory() throws Exception {
    InodeSyncStream iss1 = makeInodeSyncStream("/", false, true, -1);
    InodeSyncStream iss2 = makeInodeSyncStream("/", false, true, -1);
    assertTheSecondSyncSkipped(syncConcurrent(iss1, iss2));
    // Only load 1 level metadata
    assertEquals(1 + mNumDirsPerLevel, mFileSystemMaster.getInodeTree().getInodeCount());

    iss1 = makeInodeSyncStream("/0_0", true, true, -1);
    iss2 = makeInodeSyncStream("/0_1", true, true, -1);
    assertSyncHappenTwice(syncConcurrent(iss1, iss2));
    // Only load 1 level metadata
    assertEquals(mNumExpectedInodes, mFileSystemMaster.getInodeTree().getInodeCount());
  }

  @Test
  public void loadMetadataForDirectoryAndItsSubDirectory()
      throws Exception {
    InodeSyncStream iss1 = makeInodeSyncStream("/", true, true, -1);
    InodeSyncStream iss2 = makeInodeSyncStream("/0_1", false, true, -1);
    assertTheSecondSyncSkipped(syncConcurrent(iss1, iss2));
    assertEquals(mNumExpectedInodes, mFileSystemMaster.getInodeTree().getInodeCount());
  }

  @Test
  public void syncTheSameDirectory() throws Exception {
    InodeSyncStream iss1 = makeInodeSyncStream("/", true, false, 0);
    InodeSyncStream iss2 = makeInodeSyncStream("/", true, false, 0);
    assertTheSecondSyncSkipped(syncConcurrent(iss1, iss2));
    assertEquals(mNumExpectedInodes, mFileSystemMaster.getInodeTree().getInodeCount());
    Supplier<InodeSyncStream> createSync = () -> makeInodeSyncStream("/", true, false, 0);
    assertSyncHappenTwice(syncSequential(createSync, createSync));

    iss1 = makeInodeSyncStream("/0_1", true, false, 0);
    iss2 = makeInodeSyncStream("/0_1", false, false, 0);
    assertTheSecondSyncSkipped(syncConcurrent(iss1, iss2));

    iss1 = makeInodeSyncStream("/0_1", true, false, -1);
    iss2 = makeInodeSyncStream("/0_1", false, false, -1);
    assertSyncNotHappen(syncConcurrent(iss1, iss2));
  }

  @Test
  public void syncDirectoryAndItsSubdirectory() throws Exception {
    InodeSyncStream iss1 = makeInodeSyncStream("/", true, false, 0);
    InodeSyncStream iss2 = makeInodeSyncStream("/0_1", true, false, 0);
    assertTheSecondSyncSkipped(syncConcurrent(iss1, iss2));
    assertEquals(mNumExpectedInodes, mFileSystemMaster.getInodeTree().getInodeCount());
    assertSyncHappenTwice(syncSequential(
        () -> makeInodeSyncStream("/", true, false, 0),
        () -> makeInodeSyncStream("/0_1", true, false, 0)));

    iss1 = makeInodeSyncStream("/", false, false, 0);
    iss2 = makeInodeSyncStream("/0_1", true, false, 0);
    assertSyncHappenTwice(syncConcurrent(iss1, iss2));

    iss1 = makeInodeSyncStream("/0_1", true, false, 0);
    iss2 = makeInodeSyncStream("/", true, false, 0);
    assertSyncHappenTwice(syncConcurrent(iss1, iss2));

    iss1 = makeInodeSyncStream("/0_1", true, false, -1);
    iss2 = makeInodeSyncStream("/", true, false, -1);
    assertSyncNotHappen(syncConcurrent(iss1, iss2));
  }

  @Test
  public void syncDifferentDirectories() throws Exception {
    InodeSyncStream iss1 = makeInodeSyncStream("/0_0", true, true, 0);
    InodeSyncStream iss2 = makeInodeSyncStream("/0_1", true, true, 0);
    assertSyncHappenTwice(syncConcurrent(iss1, iss2));

    iss1 = makeInodeSyncStream("/0_0", true, false, 0);
    iss2 = makeInodeSyncStream("/0_1", true, false, 0);
    assertSyncHappenTwice(syncConcurrent(iss1, iss2));
    assertEquals(mNumExpectedInodes, mFileSystemMaster.getInodeTree().getInodeCount());
  }

  /**
   * To test if the metadata sync cancellation will result in deadlock.
   */
  @Test
  public void syncTheSameDirectoryButTheSecondCallCancelled() throws Exception {
    InodeSyncStream iss1 = makeInodeSyncStream("/", true, false, 0);
    InodeSyncStream iss2 = makeInodeSyncStream("/", true, false, 0);
    CompletableFuture<InodeSyncStream.SyncStatus> f1 = CompletableFuture.supplyAsync(() -> {
      try {
        return iss1.sync();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    Thread.sleep(10);
    CompletableFuture<InodeSyncStream.SyncStatus> f2 = CompletableFuture.supplyAsync(() -> {
      try {
        return iss2.sync();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    Thread.sleep(100);
    f2.cancel(true);
    f1.get();
    InodeSyncStream iss3 = makeInodeSyncStream("/", true, false, 0);
    assertEquals(InodeSyncStream.SyncStatus.OK, iss3.sync());
  }

  private void assertTheSecondSyncSkipped(
      Pair<InodeSyncStream.SyncStatus, InodeSyncStream.SyncStatus> results) {
    assertEquals(InodeSyncStream.SyncStatus.OK, results.getFirst());
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, results.getSecond());
  }

  private void assertSyncHappenTwice(
      Pair<InodeSyncStream.SyncStatus, InodeSyncStream.SyncStatus> results) {
    assertEquals(InodeSyncStream.SyncStatus.OK, results.getFirst());
    assertEquals(InodeSyncStream.SyncStatus.OK, results.getSecond());
  }

  private void assertSyncNotHappen(
      Pair<InodeSyncStream.SyncStatus, InodeSyncStream.SyncStatus> results) {
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, results.getFirst());
    assertEquals(InodeSyncStream.SyncStatus.NOT_NEEDED, results.getSecond());
  }

  private InodeSyncStream makeInodeSyncStream(
      String path, boolean isRecursive, boolean forceSync, long syncInterval) {
    FileSystemMasterCommonPOptions options = FileSystemMasterCommonPOptions.newBuilder()
        .setSyncIntervalMs(syncInterval)
        .build();
    DescendantType descendantType = isRecursive ? DescendantType.ALL : DescendantType.ONE;
    try {
      LockingScheme syncScheme =
          new LockingScheme(new AlluxioURI(path), InodeTree.LockPattern.READ, options,
              mFileSystemMaster.getSyncPathCache(), descendantType); // shouldSync
      return
          new InodeSyncStream(syncScheme, mFileSystemMaster, mFileSystemMaster.getSyncPathCache(),
          RpcContext.NOOP, descendantType, options,
          forceSync,
          forceSync,
          forceSync);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Pair<InodeSyncStream.SyncStatus, InodeSyncStream.SyncStatus> syncConcurrent(
      InodeSyncStream iss1, InodeSyncStream iss2)
      throws ExecutionException, InterruptedException {
    Thread.sleep(10);
    CompletableFuture<InodeSyncStream.SyncStatus> f1 = CompletableFuture.supplyAsync(() -> {
      try {
        return iss1.sync();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    Thread.sleep(100);
    CompletableFuture<InodeSyncStream.SyncStatus> f2 = CompletableFuture.supplyAsync(() -> {
      try {
        return iss2.sync();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    return new Pair<>(f1.get(), f2.get());
  }

  private Pair<InodeSyncStream.SyncStatus, InodeSyncStream.SyncStatus> syncSequential(
      Supplier<InodeSyncStream> iss1, Supplier<InodeSyncStream> iss2)
      throws AccessControlException, InvalidPathException, InterruptedException {
    Thread.sleep(10);
    InodeSyncStream.SyncStatus result1 = iss1.get().sync();
    Thread.sleep(10);
    InodeSyncStream.SyncStatus result2 = iss2.get().sync();
    return new Pair<>(result1, result2);
  }

  private void createUfsHierarchy(int level, int maxLevel, String prefix, int numPerLevel)
      throws IOException {
    if (level >= maxLevel) {
      return;
    }
    for (int i = 0; i < numPerLevel; ++i) {
      String dirPath = prefix + "/" + level + "_" + i;
      createUfsDir(dirPath);
      createUfsHierarchy(level + 1, maxLevel, dirPath, numPerLevel);
    }
  }
}

