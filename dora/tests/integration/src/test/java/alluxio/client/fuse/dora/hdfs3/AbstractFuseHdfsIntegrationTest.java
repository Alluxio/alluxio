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

package alluxio.client.fuse.dora.hdfs3;

import static org.junit.Assert.fail;

import alluxio.AlluxioTestDirectory;
import alluxio.Constants;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.FileSystemOptions;
import alluxio.client.file.options.UfsFileSystemOptions;
import alluxio.client.fuse.dora.FuseUtils;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.fuse.AlluxioJniFuseFileSystem;
import alluxio.fuse.options.FuseOptions;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.HashSet;

public class AbstractFuseHdfsIntegrationTest {
  @Rule
  public TemporaryFolder mTemp = new TemporaryFolder();
  protected static final String MOUNT_POINT = AlluxioTestDirectory
      .createTemporaryDirectory("fuse_mount").toString();

  private static final String PAGING_STORE_DIR = AlluxioTestDirectory
      .createTemporaryDirectory("paging_store_dir").toString();
  private AlluxioJniFuseFileSystem mFuseFileSystem;

  // Hdfs related
  protected MiniDFSCluster mHdfsCluster;
  protected final org.apache.hadoop.conf.Configuration
      mHdfsConfiguration = new org.apache.hadoop.conf.Configuration();
  protected static final int HDFS_BLOCK_SIZE = 1024 * 1024;
  private static final int HDFS_NAMENODE_PORT = 9870;
  protected FileSystem mHdfs;

  @Rule
  public LocalAlluxioClusterResource mAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.USER_NETTY_DATA_TRANSMISSION_ENABLED, true)
          .setProperty(PropertyKey.USER_STREAMING_READER_CHUNK_SIZE_BYTES, Constants.KB)
          .setProperty(PropertyKey.FUSE_MOUNT_POINT, MOUNT_POINT)
          .setProperty(PropertyKey.WORKER_PAGE_STORE_DIRS, PAGING_STORE_DIR)
          .setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS,
              "hdfs://localhost:" + HDFS_NAMENODE_PORT + "/")
          .setProperty(PropertyKey.DORA_CLIENT_UFS_ROOT,
              "hdfs://localhost:" + HDFS_NAMENODE_PORT + "/")
          .setStartCluster(false)
          .build();

  @BeforeClass
  public static void beforeClass() {
    // Load fuse library need be implemented in individual test classes,
    // because the fuse library loading process is location dependent.
    // please add the following to the beforeClass method of test classes.
    // LibFuse.loadLibrary(AlluxioFuseUtils.getLibfuseVersion(Configuration.global()));
  }

  @Before
  public void before() throws Exception {
    initHdfsMiniCluster();
    // Alluxio cluster must start after the hdfs mini cluster is ready.s
    mAlluxioClusterResource.start();
    mountFuse();
  }

  @After
  public void after() throws Exception {
    mFuseFileSystem.umount(true);
    if (mHdfsCluster != null) {
      mHdfsCluster.shutdown();
    }
  }

  private void initHdfsMiniCluster() throws IOException {
    mHdfsConfiguration.set("dfs.name.dir", mTemp.newFolder("nn").getAbsolutePath());
    mHdfsConfiguration.set("dfs.data.dir", mTemp.newFolder("dn").getAbsolutePath());
    // 1MB block size for testing to save memory
    mHdfsConfiguration.setInt("dfs.block.size", HDFS_BLOCK_SIZE);

    mHdfsCluster = new MiniDFSCluster.Builder(mHdfsConfiguration)
        .enableManagedDfsDirsRedundancy(false)
        .manageDataDfsDirs(false)
        .manageNameDfsDirs(false)
        .nameNodePort(HDFS_NAMENODE_PORT)
        .numDataNodes(1).build();
    mHdfs = mHdfsCluster.getFileSystem();
  }

  private void mountFuse() throws IOException {
    UfsFileSystemOptions ufsOptions = new UfsFileSystemOptions("/");
    FileSystemContext fsContext = FileSystemContext.create(Configuration.global());
    final FileSystemOptions fileSystemOptions =
        FileSystemOptions.Builder
            .fromConf(Configuration.global())
            .setUfsFileSystemOptions(ufsOptions)
            .build();
    mFuseFileSystem = createJniFuseFileSystem(fsContext,
        mAlluxioClusterResource.get().getClient(),
        FuseOptions.Builder.fromConfig(Configuration.global())
            .setFileSystemOptions(fileSystemOptions)
            .setUpdateCheckEnabled(false)
            .build());
    mFuseFileSystem.mount(false, false, new HashSet<>());
    if (!FuseUtils.waitForFuseMounted(MOUNT_POINT)) {
      FuseUtils.umountFromShellIfMounted(MOUNT_POINT);
      fail("Could not setup FUSE mount point");
    }
  }

  protected AlluxioJniFuseFileSystem createJniFuseFileSystem(
      FileSystemContext fsContext, alluxio.client.file.FileSystem fs, FuseOptions fuseOptions) {
    return new AlluxioJniFuseFileSystem(fsContext, fs, fuseOptions);
  }
}
