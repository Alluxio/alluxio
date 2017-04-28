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

import alluxio.underfs.UnderFileSystemCluster;
import alluxio.util.io.FileUtils;

import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A local MiniDFSCluster for testing {@code HdfsUnderFileSystem}. This class emulates an HDFS
 * cluster on the local machine, so {@code HdfsUnderFilesystem} can talk to this emulated HDFS
 * cluster.
 */
public class LocalMiniDFSCluster extends UnderFileSystemCluster {
  private static final Logger LOG = LoggerFactory.getLogger(LocalMiniDFSCluster.class);
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

  @Override
  public void cleanup() throws IOException {
    FileUtils.deletePathRecursively(mBaseDir);
    FileUtils.createDir(mBaseDir);
  }

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

      cleanup();

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
