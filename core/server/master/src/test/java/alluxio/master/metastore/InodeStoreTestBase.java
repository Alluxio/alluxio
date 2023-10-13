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

package alluxio.master.metastore;

import static org.apache.commons.io.FileUtils.writeStringToFile;

import alluxio.AlluxioTestDirectory;
import alluxio.ConfigurationRule;
import alluxio.concurrent.LockMode;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.meta.Edge;
import alluxio.master.file.meta.InodeLockManager;
import alluxio.master.file.meta.InodeView;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.file.meta.MutableInodeDirectory;
import alluxio.master.file.meta.MutableInodeFile;
import alluxio.master.metastore.caching.CachingInodeStore;
import alluxio.master.metastore.heap.HeapInodeStore;
import alluxio.master.metastore.rocks.RocksInodeStore;
import alluxio.resource.LockResource;

import com.google.common.collect.ImmutableMap;
import io.netty.util.ResourceLeakDetector;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.function.Function;

public class InodeStoreTestBase {
  protected static final int CACHE_SIZE = 16;
  protected static String sDir;
  protected static final String CONF_NAME = "/rocks-inode.ini";

  @Parameters
  public static Iterable<Function<InodeLockManager, InodeStore>> parameters() throws Exception {
    sDir =
        AlluxioTestDirectory.createTemporaryDirectory("inode-store-test").getAbsolutePath();
    File confFile = new File(sDir + CONF_NAME);
    writeStringToFile(confFile, ROCKS_CONFIG, (Charset) null);

    return Arrays.asList(
        lockManager -> new HeapInodeStore(),
        lockManager -> new RocksInodeStore(sDir),
        lockManager -> new CachingInodeStore(new RocksInodeStore(sDir), lockManager));
  }

  @Rule
  public ConfigurationRule mConf = new ConfigurationRule(
      ImmutableMap.of(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE, CACHE_SIZE,
          PropertyKey.MASTER_METASTORE_INODE_CACHE_EVICT_BATCH_SIZE, 5,
          PropertyKey.LEAK_DETECTOR_LEVEL, ResourceLeakDetector.Level.PARANOID,
          PropertyKey.LEAK_DETECTOR_EXIT_ON_LEAK, true),
      Configuration.modifiableGlobal());

  protected final MutableInodeDirectory mRoot = inodeDir(0, -1, "");

  protected final Function<InodeLockManager, InodeStore> mStoreBuilder;
  protected InodeStore mStore;
  protected InodeLockManager mLockManager;

  public InodeStoreTestBase(Function<InodeLockManager, InodeStore> store) {
    mStoreBuilder = store;
  }

  @Before
  public void before() {
    mLockManager = new InodeLockManager();
    mStore = mStoreBuilder.apply(mLockManager);
  }

  @After
  public void after() {
    mStore.close();
  }

  protected void writeInode(MutableInode<?> inode) {
    try (LockResource lr = mLockManager.lockInode(inode, LockMode.WRITE, false)) {
      mStore.writeInode(inode);
    }
  }

  protected void writeEdge(MutableInode<?> parent, MutableInode<?> child) {
    try (LockResource lr =
             mLockManager.lockEdge(new Edge(parent.getId(), child.getName()),
                 LockMode.WRITE, false)) {
      mStore.addChild(parent.getId(), child);
    }
  }

  protected void removeInode(InodeView inode) {
    try (LockResource lr = mLockManager.lockInode(inode, LockMode.WRITE, false)) {
      mStore.remove(inode);
    }
  }

  protected void removeParentEdge(InodeView child) {
    try (LockResource lr = mLockManager
        .lockEdge(new Edge(child.getParentId(), child.getName()), LockMode.WRITE, false)) {
      mStore.removeChild(child.getParentId(), child.getName());
    }
  }

  protected static MutableInodeDirectory inodeDir(long id, long parentId, String name) {
    return MutableInodeDirectory.create(id, parentId, name, CreateDirectoryContext.defaults());
  }

  protected static MutableInodeFile inodeFile(long containerId, long parentId, String name) {
    return MutableInodeFile.create(containerId, parentId, name, 0, CreateFileContext.defaults());
  }

  // RocksDB configuration options used for the unit tests
  private static final String ROCKS_CONFIG = "[Version]\n"
      + "  rocksdb_version=7.0.3\n"
      + "  options_file_version=1.1\n"
      + "\n"
      + "[DBOptions]\n"
      + "  create_missing_column_families=true\n"
      + "  create_if_missing=true\n"
      + "\n"
      + "\n"
      + "[CFOptions \"default\"]\n"
      + "\n"
      + "  \n"
      + "[TableOptions/BlockBasedTable \"default\"]\n"
      + "\n"
      + "\n"
      + "[CFOptions \"inodes\"]\n"
      + "  \n"
      + "[TableOptions/BlockBasedTable \"inodes\"]\n"
      + "  \n"
      + "\n"
      + "[CFOptions \"edges\"]\n"
      + "  \n";
}
