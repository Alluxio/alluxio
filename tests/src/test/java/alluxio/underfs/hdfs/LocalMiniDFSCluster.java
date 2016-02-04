/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.underfs.hdfs;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import alluxio.TachyonURI;
import alluxio.conf.TachyonConf;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemCluster;
import alluxio.util.UnderFileSystemUtils;

/**
 * A local MiniDFSCluster for testing {@link HdfsUnderFileSystem}. This class emulates an HDFS
 * cluster on the local machine, so {@code HdfsUnderFilesystem} can talk to this emulated HDFS
 * cluster.
 */
public class LocalMiniDFSCluster extends UnderFileSystemCluster {
  /**
   * Test the local minidfscluster only
   */
  public static void main(String[] args) throws Exception {
    LocalMiniDFSCluster cluster = null;
    TachyonConf tachyonConf = new TachyonConf();
    try {
      cluster = new LocalMiniDFSCluster("/tmp/dfs", 1, 54321, tachyonConf);
      cluster.start();
      System.out.println("Address of local minidfscluster: " + cluster.getUnderFilesystemAddress());
      Thread.sleep(10);
      DistributedFileSystem dfs = cluster.getDFSClient();
      dfs.mkdirs(new Path("/1"));
      mkdirs(cluster.getUnderFilesystemAddress() + "/1/2", tachyonConf);
      FileStatus[] fs = dfs.listStatus(new Path(TachyonURI.SEPARATOR));
      assert fs.length != 0;
      System.out.println(fs[0].getPath().toUri());
      dfs.close();

      cluster.shutdown();

      cluster = new LocalMiniDFSCluster("/tmp/dfs", 3, tachyonConf);
      cluster.start();
      System.out.println("Address of local minidfscluster: " + cluster.getUnderFilesystemAddress());

      dfs = cluster.getDFSClient();
      dfs.mkdirs(new Path("/1"));

      UnderFileSystemUtils.touch(
          cluster.getUnderFilesystemAddress() + "/1" + "/_format_" + System.currentTimeMillis(),
          tachyonConf);
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

  public static boolean mkdirs(String path, TachyonConf tachyonConf) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path, tachyonConf);
    return ufs.mkdirs(path, true);
  }

  private Configuration mConf = new Configuration();
  private int mNamenodePort;

  private int mNumDataNode;
  private MiniDFSCluster mDfsCluster = null;

  private DistributedFileSystem mDfsClient = null;

  private boolean mIsStarted = false;

  /**
   * To intiaize the local minidfscluster with single namenode and datanode
   *
   * @param dfsBaseDirs the base directory for both namenode and datanode. The dfs.name.dir and
   *        dfs.data.dir will be setup as dfsBaseDir/name* and dfsBaseDir/data* respectively
   * @param tachyonConf the {@link alluxio.conf.TachyonConf} instance
   */
  public LocalMiniDFSCluster(String dfsBaseDirs, TachyonConf tachyonConf) {
    this(dfsBaseDirs, 1, 0, tachyonConf);
  }

  /**
   * To initialize the local minidfscluster
   *
   * @param dfsBaseDirs the base directory for both namenode and datanode. The dfs.name.dir and
   *        dfs.data.dir will be setup as dfsBaseDir/name* and dfsBaseDir/data* respectively
   * @param numDataNode the number of datanode
   * @param tachyonConf the {@link alluxio.conf.TachyonConf} instance
   */
  public LocalMiniDFSCluster(String dfsBaseDirs, int numDataNode, TachyonConf tachyonConf) {
    this(dfsBaseDirs, numDataNode, 0, tachyonConf);
  }

  /**
   * To initialize the local minidfscluster
   *
   * @param dfsBaseDirs the base directory for both namenode and datanode. The dfs.name.dir and
   *        dfs.data.dir will be setup as dfsBaseDir/name* and dfsBaseDir/data* respectively
   * @param numDataNode The number of datanode
   * @param nameNodePort the port of namenode. If it is 0, the real namenode port can be retrieved
   *        by {@link #getNameNodePort()} after the cluster started
   * @param tachyonConf the {@link alluxio.conf.TachyonConf} instance
   */
  public LocalMiniDFSCluster(String dfsBaseDirs, int numDataNode, int nameNodePort,
      TachyonConf tachyonConf) {
    super(dfsBaseDirs, tachyonConf);
    mNamenodePort = nameNodePort;
    mNumDataNode = numDataNode;
  }

  /**
   * To initialize the local minidfscluster
   *
   * @param conf the base configuration to use in starting the servers. This will be modified as
   *        necessary.
   * @param dfsBaseDirs the base directory for both namenode and datanode. The dfs.name.dir and
   *        dfs.data.dir will be setup as dfsBaseDir/name* and dfsBaseDir/data* respectively
   * @param numDataNode the number of datanode
   * @param nameNodePort the port of namenode. If it is 0, the real namenode port can be retrieved
   *        by {@link #getNameNodePort()} after the cluster started
   * @param tachyonConf the {@link alluxio.conf.TachyonConf} instance
   */
  public LocalMiniDFSCluster(Configuration conf, String dfsBaseDirs, int numDataNode,
      int nameNodePort, TachyonConf tachyonConf) {
    super(dfsBaseDirs, tachyonConf);
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
   * Get the specified or real namenode port
   *
   * @return port of namenode
   */
  public int getNameNodePort() {
    return mNamenodePort;
  }

  /**
   * Get the namenode address for this minidfscluster
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
   * To shutdown the minidfscluster in teardown phase.
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
   * To start the minidfscluster before using it
   *
   * @throws IOException
   */
  @Override
  public void start() throws IOException {
    if (!mIsStarted) {

      delete(mBaseDir, true);
      if (!mkdirs(mBaseDir, mTachyonConf)) {
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
