/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.hdfs;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemCluster;
import alluxio.util.UnderFileSystemUtils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import java.io.File;
import java.io.IOException;

/**
 * A local MiniDFSCluster for testing {@code HdfsUnderFileSystem}. This class emulates an HDFS
 * cluster on the local machine, so {@code HdfsUnderFilesystem} can talk to this emulated HDFS
 * cluster.
 */
public class LocalMiniDFSCluster extends UnderFileSystemCluster {
  /**
   * Tests the local minidfscluster only.
   */
  public static void main(String[] args) throws Exception {
    LocalMiniDFSCluster cluster = null;
    Configuration configuration = new Configuration();
    try {
      cluster = new LocalMiniDFSCluster("/tmp/dfs", 1, 54321, configuration);
      cluster.start();
      System.out.println("Address of local minidfscluster: " + cluster.getUnderFilesystemAddress());
      Thread.sleep(10);
      DistributedFileSystem dfs = cluster.getDFSClient();
      dfs.mkdirs(new Path("/1"));
      mkdirs(cluster.getUnderFilesystemAddress() + "/1/2", configuration);
      FileStatus[] fs = dfs.listStatus(new Path(AlluxioURI.SEPARATOR));
      assert fs.length != 0;
      System.out.println(fs[0].getPath().toUri());
      dfs.close();

      cluster.shutdown();

      cluster = new LocalMiniDFSCluster("/tmp/dfs", 3, configuration);
      cluster.start();
      System.out.println("Address of local minidfscluster: " + cluster.getUnderFilesystemAddress());

      dfs = cluster.getDFSClient();
      dfs.mkdirs(new Path("/1"));

      UnderFileSystemUtils.touch(
          cluster.getUnderFilesystemAddress() + "/1" + "/_format_" + System.currentTimeMillis(),
          configuration);
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

  public static boolean mkdirs(String path, Configuration configuration) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path, configuration);
    return ufs.mkdirs(path, true);
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
   * @param configuration the {@link Configuration} instance
   */
  public LocalMiniDFSCluster(String dfsBaseDirs, Configuration configuration) {
    this(dfsBaseDirs, 1, 0, configuration);
  }

  /**
   * Creates a new {@link LocalMiniDFSCluster}.
   *
   * @param dfsBaseDirs the base directory for both namenode and datanode. The dfs.name.dir and
   *        dfs.data.dir will be setup as dfsBaseDir/name* and dfsBaseDir/data* respectively
   * @param numDataNode the number of datanode
   * @param configuration the {@link Configuration} instance
   */
  public LocalMiniDFSCluster(String dfsBaseDirs, int numDataNode, Configuration configuration) {
    this(dfsBaseDirs, numDataNode, 0, configuration);
  }

  /**
   * Creates a new {@link LocalMiniDFSCluster}.
   *
   * @param dfsBaseDirs the base directory for both namenode and datanode. The dfs.name.dir and
   *        dfs.data.dir will be setup as dfsBaseDir/name* and dfsBaseDir/data* respectively
   * @param numDataNode The number of datanode
   * @param nameNodePort the port of namenode. If it is 0, the real namenode port can be retrieved
   *        by {@link #getNameNodePort()} after the cluster started
   * @param configuration the {@link Configuration} instance
   */
  public LocalMiniDFSCluster(String dfsBaseDirs, int numDataNode, int nameNodePort,
      Configuration configuration) {
    super(dfsBaseDirs, configuration);
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
   * @param configuration the {@link Configuration} instance
   */
  public LocalMiniDFSCluster(org.apache.hadoop.conf.Configuration conf, String dfsBaseDirs,
      int numDataNode, int nameNodePort, Configuration configuration) {
    super(dfsBaseDirs, configuration);
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
   * Gets the namenode address for this {@LocalMiniDFSCluster}.
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
   *
   * @throws IOException
   */
  @Override
  public void shutdown() throws IOException {
    if (mIsStarted) {
      mDfsClient.close();
      mDfsCluster.shutdown();
      mIsStarted = false;
    }
  }

  /**
   * Starts the minidfscluster before using it.
   *
   * @throws IOException
   */
  @Override
  public void start() throws IOException {
    if (!mIsStarted) {

      delete(mBaseDir, true);
      if (!mkdirs(mBaseDir, mConfiguration)) {
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
