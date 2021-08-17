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

package alluxio.client.fs.concurrent;

import static junit.framework.TestCase.assertTrue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.master.journal.JournalType;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystem.Factory;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Tests the correctness of concurrent recursive creates.
 */
public class ConcurrentRecursiveCreateIntegrationTest extends BaseIntegrationTest {
  private static final int NUM_TOP_LEVEL_DIRS = 10;

  @Rule
  public LocalAlluxioClusterResource mClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS.toString()).build();

  @Test
  public void createDuringUfsRename() throws Exception {
    FileSystem fs = mClusterResource.get().getClient();
    ExecutorService executor = Executors.newCachedThreadPool();
    UnderFileSystem ufs = Factory.createForRoot(ServerConfiguration.global());
    String ufsRoot = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    List<String> paths = new ArrayList<>();
    for (int i = 0; i < NUM_TOP_LEVEL_DIRS / 2; i++) {
      String alluxioPath = PathUtils.concatPath("/dir" + i, "a", "b", "c");
      ufs.mkdirs(PathUtils.concatPath(ufsRoot, alluxioPath));
      paths.add(alluxioPath);
    }
    executor.submit(new UfsRenamer(ufs, ufsRoot));
    for (int i = 0; i < 10; i++) {
      executor.submit(new AlluxioCreator(fs, paths));
    }
    CommonUtils.sleepMs(2 * Constants.SECOND_MS);
    executor.shutdownNow();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    mClusterResource.get().restartMasters();
    fs = mClusterResource.get().getClient();
    fs.listStatus(new AlluxioURI("/"));
  }

  private static class UfsRenamer implements Callable<Void> {
    private final UnderFileSystem mUfs;
    private final String mUfsRoot;

    public UfsRenamer(UnderFileSystem ufs, String ufsRoot) {
      mUfs = ufs;
      mUfsRoot = ufsRoot;
    }

    @Override
    public Void call() throws Exception {
      while (!Thread.interrupted()) {
        String src = PathUtils.concatPath(mUfsRoot,
            "dir" + ThreadLocalRandom.current().nextInt(NUM_TOP_LEVEL_DIRS));
        String dst = PathUtils.concatPath(mUfsRoot,
            "dir" + ThreadLocalRandom.current().nextInt(NUM_TOP_LEVEL_DIRS));
        if (mUfs.exists(src) && !mUfs.exists(dst)) {
          mUfs.renameDirectory(src, dst);
        }
      }
      return null;
    }
  }

  private static class AlluxioCreator implements Callable<Void> {
    private final FileSystem mFs;
    private final List<String> mPaths;

    public AlluxioCreator(FileSystem fs, List<String> paths) {
      mFs = fs;
      mPaths = paths;
    }

    @Override
    public Void call() {
      while (!Thread.interrupted()) {
        int ind = ThreadLocalRandom.current().nextInt(mPaths.size());
        String path = mPaths.get(ind);
        try {
          mFs.createDirectory(new AlluxioURI(path),
              CreateDirectoryPOptions.newBuilder().setRecursive(true).setAllowExists(true).build());
          while (!PathUtils.isRoot(PathUtils.getParent(path))) {
            path = PathUtils.getParent(path);
          }
          mFs.delete(new AlluxioURI(path), DeletePOptions.newBuilder().setRecursive(true).build());
        } catch (Exception e) {
          continue;
        }
      }
      return null;
    }
  }
}
