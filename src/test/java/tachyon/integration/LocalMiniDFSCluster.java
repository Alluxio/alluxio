/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.integration;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import tachyon.conf.CommonConf;
import tachyon.master.LocalUnderFilesystemCluster;
import tachyon.util.CommonUtils;

/**
 * A local MiniDFSCluster for test hadoop-ufs.
 */
public class LocalMiniDFSCluster extends LocalUnderFilesystemCluster {
  private int mNamenodePort;
  private int mNumDataNode;
  private final Configuration conf = new Configuration();

  private String mHdfsAddress = null;

  private MiniDFSCluster mDfsCluster = null;
  private DistributedFileSystem mDfsClient = null;

  private boolean mIsStarted = false;

  /**
   * To initialize the local minidfscluster
   *
   * @param dfsBaseDirs
   *          The base directory for both namenode and datanode. The
   *          dfs.name.dir and dfs.data.dir will be setup as dfsBaseDir/name*
   *          and dfsBaseDir/data* respectively
   * @param numDataNode
   *          The number of datanode
   */
  public LocalMiniDFSCluster(String dfsBaseDirs, int numDataNode) {
    this(dfsBaseDirs, numDataNode, 0);
  }

  /**
   * To intiaize the local minidfscluster with single namenode and datanode
   *
   * @param dfsBaseDirs
   *          The base directory for both namenode and datanode. The
   *          dfs.name.dir and dfs.data.dir will be setup as dfsBaseDir/name*
   *          and dfsBaseDir/data* respectively
   */
  public LocalMiniDFSCluster(String dfsBaseDirs) {
    this(dfsBaseDirs, 1, 0);
  }

  /**
   * To initialize the local minidfscluster
   *
   * @param dfsBaseDirs
   *          The base directory for both namenode and datanode. The
   *          dfs.name.dir and dfs.data.dir will be setup as dfsBaseDir/name*
   *          and dfsBaseDir/data* respectively
   * @param numDataNode
   *          The number of datanode
   * @param nameNodePort
   *          The port of namenode. If it is 0, the real namenode port can be
   *          retrieved by {@link #getNameNodePort()} after the cluster started
   */
  public LocalMiniDFSCluster(String dfsBaseDirs, int numDataNode, int nameNodePort) {
    super(dfsBaseDirs);
    this.mNamenodePort = nameNodePort;
    this.mNumDataNode = numDataNode;
  }

  /**
   * Get the specified or real namenode port
   *
   * @return port of namenode
   */
  public int getNameNodePort() {
    return this.mNamenodePort;
  }

  /**
   * Get the namenode address for this minidfscluster
   *
   * @return namenode address
   */
  public String getUnderFilesystemAddress() {
    if (null != this.mDfsClient) {
      return this.mDfsClient.getUri().toString();
    } else {
      return null;
    }
  }

  /**
   * To start the minidfscluster before using it
   *
   * @throws IOException
   */
  @Override
  public void start() throws IOException {
    if (!this.mIsStarted) {

      delete(mBaseDir, true);
      mkdir(mBaseDir);

      // TODO For hadoop 1.x, there exists NPE while startDataNode. It is a
      // known issue caused by "umask 002"(should be 022) see [HDFS-2556]. So
      // the following codes only work for hadoop 2.x or "umask 022"
      System.setProperty("test.build.data", mBaseDir);
      mDfsCluster = new MiniDFSCluster(this.mNamenodePort, conf, this.mNumDataNode, true, true,
          null, null);

      mDfsCluster.waitClusterUp();

      if (0 == this.mNamenodePort) {
        this.mNamenodePort = mDfsCluster.getNameNodePort();
      }

      mDfsClient = (DistributedFileSystem) mDfsCluster.getFileSystem();
      mIsStarted = true;

    }
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

  @Override
  public boolean isStarted() {
    return mIsStarted;
  }

  public DistributedFileSystem getDFSClient() {
    return this.mDfsClient;
  }

  public Configuration getConf() {
    return this.conf;
  }

  private void mkdir(String path) throws IOException {
    if (!CommonUtils.mkdirs(path)) {
      throw new IOException("Failed to make folder: " + path);
    }
  }

  private void delete(String path, boolean isRecursively) throws IOException {
    File p = new File(path);
    if (isRecursively && p.isDirectory()) {
      for (File subdir : p.listFiles()) {
        delete(subdir.getAbsolutePath(), isRecursively);
      }
    }
    p.delete();
  }

  /**
   * Test the local minidfscluster only
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
      CommonUtils.mkdirs(cluster.getUnderFilesystemAddress() + "/1/2");
      FileStatus[] fs = dfs.listStatus(new Path("/"));
      assert fs.length != 0;
      System.out.println(fs[0].getPath().toUri());
      dfs.close();

      cluster.shutdown();

      cluster = new LocalMiniDFSCluster("/tmp/dfs", 3);
      cluster.start();
      System.out.println("Address of local minidfscluster: " + cluster.getUnderFilesystemAddress());

      dfs = cluster.getDFSClient();
      dfs.mkdirs(new Path("/1"));

      CommonUtils.touch(cluster.getUnderFilesystemAddress() + "/1" + "/_format_"
          + System.currentTimeMillis());
      fs = dfs.listStatus(new Path("/1"));
      assert fs.length != 0;
      System.out.println(fs[0].getPath().toUri());
      dfs.close();

      cluster.shutdown();
    } finally {
      if (cluster != null && cluster.isStarted())
        cluster.shutdown();
    }

  }
}
