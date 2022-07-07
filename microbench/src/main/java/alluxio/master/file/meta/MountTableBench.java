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
import alluxio.conf.Configuration;
import alluxio.exception.InvalidPathException;
import alluxio.master.NoopUfsManager;
import alluxio.master.file.contexts.MountContext;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.journal.NoopJournalContext;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.local.LocalUnderFileSystemFactory;
import alluxio.util.IdUtils;
import alluxio.util.io.PathUtils;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.ArrayList;
import java.util.List;

/**
 * MountTableBench test the performance of methods of {@link MountTable}.
 */
public class MountTableBench {
  @State(Scope.Benchmark)
  public static class BenchState extends BaseInodeState {
    public MountTable mMountTable = null;
    public InodeDirectory mDirWidth = null;
    public InodeDirectory mDirDepth = null;
    // mWidthDirectories includes several directories that are under the same directory, i.e.
    // /dir/1, /dir/2, /dir/3.
    public List<InodeDirectory> mWidthDirectories = new ArrayList<>();
    public List<LockedInodePath> mAlluxioWidthMountPath = new ArrayList<>();
    public List<String> mUfsDepthMountedPaths = new ArrayList<>();

    // mDepthDirectories includes several directories that are the parent directory of the next
    // element(if exists), i.e. /dir/1, /dir/1/2, /dir/1/2/3.
    public List<InodeDirectory> mDepthDirectories = new ArrayList<>();
    public List<LockedInodePath> mAlluxioDepthMountPath = new ArrayList<>();
    public List<String> mUfsWidthMountedPaths = new ArrayList<>();

    // index used to indicate the find the target test path
    @Param({"0", "2", "4"})
    public int mDepthGetMountPointTargetIndex;

    @Param({"2"})
    public int mWidthGetMountPointTargetIndex;

    @Param({"0", "2", "4"})
    public int mDepthFindChildrenMountPointsTargetIndex;

    @Param({"0"})
    public int mWidthFindChildrenMountPointsTargetIndex;

    // target test paths for each test
    public LockedInodePath mWidthGetMountPointTarget = null;
    public LockedInodePath mDepthGetMountPointTarget = null;
    public LockedInodePath mWidthFindChildrenMountPointsTarget = null;
    public LockedInodePath mDepthFindChildrenMountPointsTarget = null;

    // mMountId is used to be a placeholder when creating a mount point, it will be increased by 1
    // after each creation.
    private int mMountId = 2;

    private static final int DEPTH = 5;
    private static final int WIDTH = 5;
    private static final String ROOT_UFS = "s3a://bucket/";
    private static final String MOUNT_UFS_WIDTH_PREFIX = "hdfs://localhost:1234/width";
    private static final String MOUNT_UFS_DEPTH_PARENT = "hdfs://localhost:1234/depth";
    private static final String ALLUXIO_WIDTH_MOUNT_PARENT = "/mnt/width";
    private static final String ALLUXIO_DEPTH_MOUNT_PARENT = "/mnt/depth";

    @Setup(Level.Trial)
    public void before() throws Exception {
      UfsManager ufsManager = new NoopUfsManager();
      mMountTable = new MountTable(ufsManager,
          new MountInfo(new AlluxioURI(MountTable.ROOT), new AlluxioURI(ROOT_UFS),
              IdUtils.ROOT_MOUNT_ID, MountContext.defaults().getOptions().build()));
      // uncomment the below line to enable the MountTableTrie for microbenchmarking
      // mMountTable.enableMountTableTrie(mRootDir);

      // create /mnt/width
      mDirDepth = inodeDir(mInodes.size(), mDirMnt.getId(), "depth");
      mInodes.add(mDirDepth);
      mInodeStore.addChild(mDirMnt.getId(), mDirDepth);
      InodeDirectory prev = mDirDepth;
      // create depth directory /mnt/width/0/1/2/3/4
      for (int i = 0; i < DEPTH; i++) {
        InodeDirectory depthDir = inodeDir(mInodes.size(), prev.getId(), Integer.toString(i));
        mDepthDirectories.add(depthDir);
        mInodes.add(depthDir);
        mInodeStore.addChild(prev.getId(), depthDir);
        prev = depthDir;
      }
      // mount (/mnt/0/1/2/3/4, hdfs://localhost:1234/0/1/2/3/4)
      String prevPath = ALLUXIO_DEPTH_MOUNT_PARENT;
      for (int i = 0; i < DEPTH; i++) {
        prevPath = PathUtils.concatPath(prevPath, Integer.toString(i));
        String ufsPath = PathUtils.concatUfsPath(MOUNT_UFS_DEPTH_PARENT, Integer.toString(i));
        LockedInodePath lockedPath = addMount(prevPath, ufsPath, mMountId++);
        mAlluxioDepthMountPath.add(lockedPath);
        mUfsDepthMountedPaths.add(ufsPath);
      }

      // create /mnt/width
      mDirWidth = inodeDir(mInodes.size(), mDirMnt.getId(), "width");
      mInodes.add(mDirWidth);
      mInodeStore.addChild(mDirMnt.getId(), mDirWidth);
      // create /mnt/foo/[0,1,2,3,4]
      for (int i = 0; i < WIDTH; i++) {
        String filename = Integer.toString(i);
        InodeDirectory file = inodeDir(mInodes.size(), mDirWidth.getId(), filename);
        mInodes.add(file);
        mWidthDirectories.add(file);
        mInodeStore.addChild(mDirWidth.getId(), file);
      }
      // mount (/mnt/width/[0,1,2,3,4], hdfs://localhost:1234/width/[0,1,2,3,4])
      for (int i = 0; i < mWidthDirectories.size(); i++) {
        String ufsPath = PathUtils.concatUfsPath(MOUNT_UFS_WIDTH_PREFIX, Integer.toString(i));
        String alluxioPath = PathUtils.concatPath(ALLUXIO_WIDTH_MOUNT_PARENT, Integer.toString(i));
        LockedInodePath lockedAlluxioPath = addMount(alluxioPath, ufsPath, mMountId++);
        mAlluxioWidthMountPath.add(lockedAlluxioPath);
        mUfsWidthMountedPaths.add(ufsPath);
      }
      // initialize the test targets
      mWidthGetMountPointTarget = mAlluxioWidthMountPath.get(mWidthGetMountPointTargetIndex);
      mDepthGetMountPointTarget = mAlluxioDepthMountPath.get(mDepthGetMountPointTargetIndex);
      mWidthFindChildrenMountPointsTarget =
          mAlluxioWidthMountPath.get(mWidthFindChildrenMountPointsTargetIndex);
      mDepthFindChildrenMountPointsTarget =
          mAlluxioDepthMountPath.get(mDepthFindChildrenMountPointsTargetIndex);
    }

    private LockedInodePath addMount(String alluxio, String ufs, long id) throws Exception {
      LockedInodePath inodePath = createLockedInodePath(alluxio);
      mMountTable.add(NoopJournalContext.INSTANCE, inodePath, new AlluxioURI(ufs), id,
          MountContext.defaults().getOptions().build());
      return inodePath;
    }

    private LockedInodePath createLockedInodePath(String path)
        throws InvalidPathException {
      LockedInodePath lockedPath = new LockedInodePath(new AlluxioURI(path), mInodeStore,
          mInodeLockManager, mRootDir, InodeTree.LockPattern.READ, false);
      lockedPath.traverse();
      return lockedPath;
    }
  }

  @Benchmark @BenchmarkMode(Mode.Throughput)
  public void testWidthGetMountPoint(BenchState state) throws InvalidPathException {
    state.mMountTable.getMountPoint(state.mWidthGetMountPointTarget);
  }

  @Benchmark @BenchmarkMode(Mode.Throughput)
  public void testDepthGetMountPoint(BenchState state) throws InvalidPathException {
    state.mMountTable.getMountPoint(state.mDepthGetMountPointTarget);
  }

  @Benchmark @BenchmarkMode(Mode.Throughput)
  public void testWidthFindChildrenMountPoint(BenchState state) throws InvalidPathException {
    state.mMountTable.findChildrenMountPoints(state.mWidthFindChildrenMountPointsTarget,
        true);
  }

  @Benchmark @BenchmarkMode(Mode.Throughput)
  public void testDepthFindChildrenMountPoint(BenchState state) throws InvalidPathException {
    state.mMountTable.findChildrenMountPoints(state.mDepthFindChildrenMountPointsTarget,
        true);
  }
}
