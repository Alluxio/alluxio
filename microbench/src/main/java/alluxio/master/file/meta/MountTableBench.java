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
import alluxio.exception.InvalidPathException;
import alluxio.master.NoopUfsManager;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.MountContext;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.journal.NoopJournalContext;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.heap.HeapInodeStore;
import alluxio.underfs.UfsManager;
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

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * MountTableBench test the performance of methods of {@link MountTable}.
 */
public class MountTableBench {
  @State(Scope.Benchmark)
  public static class BenchState {
    /** Bench preparation utils. */
    private final InodeLockManager mInodeLockManager = new InodeLockManager();
    private final InodeStore mInodeStore = new HeapInodeStore();

    private final InodeDirectory mDirMnt = inodeDir(1, 0, "mnt");
    private final InodeDirectory mRootDir = inodeDir(0, -1, "", mDirMnt);
    private final List<Inode> mInodes = new ArrayList<>(Arrays.asList(mRootDir, mDirMnt));

    public InodeDirectory createInodeDir(InodeDirectory parentDir, String name) {
      InodeDirectory dir = inodeDir(mInodes.size(), parentDir.getId(), name);
      mInodes.add(dir);
      mInodeStore.addChild(parentDir.getId(), dir);
      return dir;
    }

    public InodeFile createInodeFile(InodeDirectory parentDir, String name) {
      InodeFile dir = inodeFile(mInodes.size(), parentDir.getId(), name);
      mInodes.add(dir);
      mInodeStore.addChild(parentDir.getId(), dir);
      return dir;
    }

    private InodeDirectory inodeDir(long id, long parentId, String name, Inode... children) {
      MutableInodeDirectory dir =
          MutableInodeDirectory.create(id, parentId, name, CreateDirectoryContext.defaults());
      mInodeStore.writeInode(dir);
      for (Inode child : children) {
        mInodeStore.addChild(dir.getId(), child);
      }
      return Inode.wrap(dir).asDirectory();
    }

    private InodeFile inodeFile(long id, long parentId, String name) {
      MutableInodeFile file =
          MutableInodeFile.create(id, parentId, name, 0, CreateFileContext.defaults());
      mInodeStore.writeInode(file);
      return Inode.wrap(file).asFile();
    }

    /** Bench target directories. */
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
    @Param({"4"})
    public int mDepthGetMountPointTargetIndex;

    @Param({"2"})
    public int mWidthGetMountPointTargetIndex;

    @Param({"4"})
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
              IdUtils.ROOT_MOUNT_ID, MountContext.defaults().getOptions().build()),
              Clock.systemUTC());
      // enable the MountTableTrie for microbenchmarking
      mMountTable.buildMountTableTrieFromRoot(mRootDir);

      // create /mnt/depth
      mDirDepth = createInodeDir(mDirMnt, "depth");

      InodeDirectory prev = mDirDepth;
      // create depth directory /mnt/width/0/1/2/3/4
      for (int i = 0; i < DEPTH; i++) {
        InodeDirectory depthDir = createInodeDir(prev, Integer.toString(i));
        mDepthDirectories.add(depthDir);
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
      mDirWidth = createInodeDir(mDirMnt, "width");
      // create /mnt/foo/[0,1,2,3,4]
      for (int i = 0; i < WIDTH; i++) {
        InodeDirectory file = createInodeDir(mDirWidth, Integer.toString(i));
        mWidthDirectories.add(file);
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
          mInodeLockManager, mRootDir, InodeTree.LockPattern.READ, false, NoopJournalContext.INSTANCE);
      lockedPath.traverse();
      return lockedPath;
    }
  }

  @Benchmark @BenchmarkMode(Mode.Throughput)
  public void testWidthGetMountPoint(BenchState state) throws InvalidPathException {
    state.mMountTable.resolveMountPointTrie(state.mWidthGetMountPointTarget);
  }

  @Benchmark @BenchmarkMode(Mode.Throughput)
  public void testDepthGetMountPoint(BenchState state) throws InvalidPathException {
    state.mMountTable.resolveMountPointTrie(state.mDepthGetMountPointTarget);
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
