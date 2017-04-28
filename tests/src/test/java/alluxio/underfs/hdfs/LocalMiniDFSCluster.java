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

package alluxio.underfs.hdfs;

import alluxio.AlluxioURI;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemCluster;
import alluxio.util.UnderFileSystemUtils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * A local MiniDFSCluster for testing {@code HdfsUnderFileSystem}. This class emulates an HDFS
 * cluster on the local machine, so {@code HdfsUnderFilesystem} can talk to this emulated HDFS
 * cluster.
 */
public class LocalMiniDFSCluster extends UnderFileSystemCluster {
  private static final Logger LOG = LoggerFactory.getLogger(LocalMiniDFSCluster.class);
  /**
   * Tests the local minidfscluster only.
   */
  public static void main(String[] args) throws Exception {
    LocalMiniDFSCluster cluster = null;
    try {
      cluster = new LocalMiniDFSCluster("/tmp/dfs", 1, 54321);
      cluster.start();
      System.out.println("Address of local minidfscluster: " + cluster.getUnderFilesystemAddress());
      Thread.sleep(10);
      DistributedFileSystem dfs = cluster.getDFSClient();
      dfs.mkdirs(new Path("/1"));
      mkdirs(cluster.getUnderFilesystemAddress() + "/1/2");
      FileStatus[] fs = dfs.listStatus(new Path(AlluxioURI.SEPARATOR));
      assert fs.length != 0;
      System.out.println(fs[0].getPath().toUri());
      dfs.close();

      cluster.shutdown();

      cluster = new LocalMiniDFSCluster("/tmp/dfs", 3);
      cluster.start();
      System.out.println("Address of local minidfscluster: " + cluster.getUnderFilesystemAddress());

      dfs = cluster.getDFSClient();
      dfs.mkdirs(new Path("/1"));

      UnderFileSystemUtils.touch(
          cluster.getUnderFilesystemAddress() + "/1" + "/_format_" + System.currentTimeMillis());
      fs = dfs.listStatus(new Path("/1"));
      assert fs.length != 0;
      System.out.println(fs[0].getPath().toUri());
      dfs.close();

      cluster.shutdown();
    } finally {
      if (cluster != null && cluster.isStarted()) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Creates a directory in the under filesystem.
   *
   * @param path the directory path to be created
   * @return {@code true} if and only if the directory was created; {@code false} otherwise
   */
  public static boolean mkdirs(String path) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.Factory.get(path);
    return ufs.mkdirs(path);
  }

  private org.apache.hadoop.conf.Configuration mConf = new org.apache.hadoop.conf.Configuration();
  private int mNamenodePort;

  private int mNumDataNode;
  private MiniDFSCluster mDfsCluster = null;

  private DistributedFileSystem mDfsClient = null;

  private boolean mIsStarted = false;

  /**
   * Initializes a {@link LocalMiniDFSCluster with a single namenode and datanode.
   *
   * @param dfsBaseDirs the base directory for both namenode and datanode. The dfs.name.dir and
   *        dfs.data.dir will be setup as dfsBaseDir/name* and dfsBaseDir/data* respectively
   */
  public LocalMiniDFSCluster(String dfsBaseDirs) {
    this(dfsBaseDirs, 1, 0);
  }

  /**
   * Creates a new {@link LocalMiniDFSCluster}.
   *
   * @param dfsBaseDirs the base directory for both namenode and datanode. The dfs.name.dir and
   *        dfs.data.dir will be setup as dfsBaseDir/name* and dfsBaseDir/data* respectively
   * @param numDataNode the number of datanode
   */
  public LocalMiniDFSCluster(String dfsBaseDirs, int numDataNode) {
    this(dfsBaseDirs, numDataNode, 0);
  }

  /**
   * Creates a new {@link LocalMiniDFSCluster}.
   *
   * @param dfsBaseDirs the base directory for both namenode and datanode. The dfs.name.dir and
   *        dfs.data.dir will be setup as dfsBaseDir/name* and dfsBaseDir/data* respectively
   * @param numDataNode The number of datanode
   * @param nameNodePort the port of namenode. If it is 0, the real namenode port can be retrieved
   *        by {@link #getNameNodePort()} after the cluster started
   */
  public LocalMiniDFSCluster(String dfsBaseDirs, int numDataNode, int nameNodePort) {
    super(dfsBaseDirs);
    mNamenodePort = nameNodePort;
    mNumDataNode = numDataNode;
  }

  /**
   * Creates a new {@link LocalMiniDFSCluster}.
   *
   * @param conf the base configuration to use in starting the servers. This will be modified as
   *        necessary.
   * @param dfsBaseDirs the base directory for both namenode and datanode. The dfs.name.dir and
   *        dfs.data.dir will be setup as dfsBaseDir/name* and dfsBaseDir/data* respectively
   * @param numDataNode the number of datanode
   * @param nameNodePort the port of namenode. If it is 0, the real namenode port can be retrieved
   *        by {@link #getNameNodePort()} after the cluster started
   */
  public LocalMiniDFSCluster(org.apache.hadoop.conf.Configuration conf, String dfsBaseDirs,
      int numDataNode, int nameNodePort) {
    super(dfsBaseDirs);
    mConf = conf;
    mNamenodePort = nameNodePort;
    mNumDataNode = numDataNode;
  }

  private void delete(String path, boolean isRecursively) throws IOException {
    File file = new File(path);
    if (isRecursively && file.isDirectory()) {
      for (File subdir : file.listFiles()) {
        delete(subdir.getAbsolutePath(), isRecursively);
      }
    }
    file.delete();
  }

  /**
   * @return {@link #mDfsClient}
   */
  public DistributedFileSystem getDFSClient() {
    return mDfsClient;
  }

  /**
   * Gets the specified or real namenode port.
   *
   * @return port of namenode
   */
  public int getNameNodePort() {
    return mNamenodePort;
  }

  /**
   * Gets the namenode address for this {@link LocalMiniDFSCluster}.
   *
   * @return namenode address
   */
  @Override
  public String getUnderFilesystemAddress() {
    if (mDfsClient != null) {
      return mDfsClient.getUri().toString();
    }
    return null;
  }

  @Override
  public boolean isStarted() {
    return mIsStarted;
  }

  /**
   * Shuts down the minidfscluster in teardown phase.
   */
  @Override
  public void shutdown() throws IOException {
    LOG.info("Shutting down DFS cluster.");
    if (mIsStarted) {
      mDfsClient.close();
      mDfsCluster.shutdown();
      mIsStarted = false;
    }
  }

  /**
   * Starts the minidfscluster before using it.
   */
  @Override
  public void start() throws IOException {
    LOG.info("Starting DFS cluster.");
    if (!mIsStarted) {

      delete(mBaseDir, true);
      if (!mkdirs(mBaseDir)) {
        throw new IOException("Failed to make folder: " + mBaseDir);
      }

      // TODO(hy): For hadoop 1.x, there exists NPE while startDataNode. It is a known issue caused
      // by "umask 002" (should be 022) see [HDFS-2556]. So the following code only works for
      // hadoop 2.x or "umask 022".
      System.setProperty("test.build.data", mBaseDir);
      mDfsCluster = new MiniDFSCluster(mNamenodePort, mConf, mNumDataNode, true, true, null, null);
      mDfsCluster.waitClusterUp();

      if (0 == mNamenodePort) {
        mNamenodePort = mDfsCluster.getNameNodePort();
      }

      // For HDFS of earlier versions, getFileSystem() returns an instance of type
      // {@link org.apache.hadoop.fs.FileSystem} rather than {@link DistributedFileSystem}
      mDfsClient = (DistributedFileSystem) mDfsCluster.getFileSystem();
      mIsStarted = true;
    }
  }
}
