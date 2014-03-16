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
package tachyon.master;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import tachyon.Constants;
import tachyon.master.MasterInfo;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.TableColumnException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TachyonException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.log4j.Logger;

/**
 * Benchmarks for tachyon.MasterInfo
 */
public class MasterInfoBenchmark {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private MasterInfo mMasterInfo = null;
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    mLocalTachyonCluster = new LocalTachyonCluster(1000);
    mLocalTachyonCluster.start();
    mMasterInfo = mLocalTachyonCluster.getMasterInfo();
  }

  /*
   * Creates a directory tree at the given root with the given
   * depth.
   */
  public class PerfTestCreateTree implements Runnable {
    private int depth;
    private int filesPerNode;
    private int concurrencyDepth;
    private String initPath;
    private MasterInfo masterInfo;

    PerfTestCreateTree(int depth, int filesPerNode, int concurrencyDepth, String initPath,
        MasterInfo masterInfo) {
      this.depth = depth;
      this.filesPerNode = filesPerNode;
      this.concurrencyDepth = concurrencyDepth;
      this.initPath = initPath;
      this.masterInfo = masterInfo;
    }

    public void exec(int depth, int filesPerNode, int concurrencyDepth, String path)
        throws Exception {
      if (depth < 1) {
        return;
      } else if (depth == 1) {
        masterInfo._createFile(false, path, false, Constants.DEFAULT_BLOCK_SIZE_BYTE, 0);
      } else {
        masterInfo._createFile(false, path, true, 0, 0);
      }
      if (concurrencyDepth > 0) {
        ExecutorService executor = Executors.newCachedThreadPool();
        Future<?>[] futures = new Future<?>[filesPerNode];
        for (int i = 0; i < filesPerNode; i ++) {
          futures[i] =
              executor.submit(new PerfTestCreateTree(depth - 1, filesPerNode,
                  concurrencyDepth - 1, path + Constants.PATH_SEPARATOR + i, masterInfo));
        }
        for (Future<?> f : futures) {
          f.get();
        }
        executor.shutdown();
      } else {
        for (int i = 0; i < filesPerNode; i ++) {
          exec(depth - 1, filesPerNode, concurrencyDepth, path + Constants.PATH_SEPARATOR + i);
        }
      }
    }

    @Override
    public void run() {
      try {
        exec(this.depth, this.filesPerNode, this.concurrencyDepth, this.initPath);
      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
    }
  }

  /*
   * Traverses the directory at path with the given depth, running
   * getClientFileInfo and ls on all the directories and
   * getClientFileInfo on all the files.
   */
  public class PerfTestTraverseTree implements Runnable {
    private int depth;
    private int filesPerNode;
    private int concurrencyDepth;
    private String initPath;
    private MasterInfo masterInfo;

    PerfTestTraverseTree(int depth, int filesPerNode, int concurrencyDepth, String initPath,
        MasterInfo masterInfo) {
      this.depth = depth;
      this.filesPerNode = filesPerNode;
      this.concurrencyDepth = concurrencyDepth;
      this.initPath = initPath;
      this.masterInfo = masterInfo;
    }

    public void exec(int depth, int filesPerNode, int concurrencyDepth, String path)
        throws Exception {
      if (depth < 1) {
        return;
      }
      masterInfo.getClientFileInfo(path);
      masterInfo.ls(path, false);
      if (depth > 1) {
        masterInfo.ls(path, false);
      }
      if (concurrencyDepth > 0) {
        ExecutorService executor = Executors.newCachedThreadPool();
        Future<?>[] futures = new Future<?>[filesPerNode];
        for (int i = 0; i < filesPerNode; i ++) {
          futures[i] =
              executor.submit(new PerfTestTraverseTree(depth - 1, filesPerNode,
                  concurrencyDepth - 1, path + Constants.PATH_SEPARATOR + i, masterInfo));
        }
        for (Future<?> f : futures) {
          f.get();
        }
        executor.shutdown();
      } else {
        for (int i = 0; i < filesPerNode; i ++) {
          exec(depth - 1, filesPerNode, concurrencyDepth, path + Constants.PATH_SEPARATOR + i);
        }
      }
    }

    @Override
    public void run() {
      try {
        exec(this.depth, this.filesPerNode, this.concurrencyDepth, this.initPath);
      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
    }
  }

  /*
   * Traverses the directory at path with the given depth, deleting
   * all the files it encounters bottom up.
   */
  public class PerfTestDeleteTree implements Runnable {
    private int depth;
    private int filesPerNode;
    private int concurrencyDepth;
    private String initPath;
    private MasterInfo masterInfo;

    PerfTestDeleteTree(int depth, int filesPerNode, int concurrencyDepth, String initPath,
        MasterInfo masterInfo) {
      this.depth = depth;
      this.filesPerNode = filesPerNode;
      this.concurrencyDepth = concurrencyDepth;
      this.initPath = initPath;
      this.masterInfo = masterInfo;
    }

    public void exec(int depth, int filesPerNode, int concurrencyDepth, String path)
        throws Exception {
      if (depth < 1) {
        return;
      }
      if (concurrencyDepth > 0) {
        ExecutorService executor = Executors.newCachedThreadPool();
        Future<?>[] futures = new Future<?>[filesPerNode];
        for (int i = 0; i < filesPerNode; i ++) {
          futures[i] =
              executor.submit(new PerfTestDeleteTree(depth - 1, filesPerNode,
                  concurrencyDepth - 1, path + Constants.PATH_SEPARATOR + i, masterInfo));
        }
        for (Future<?> f : futures) {
          f.get();
        }
        executor.shutdown();
      } else {
        for (int i = 0; i < filesPerNode; i ++) {
          exec(depth - 1, filesPerNode, concurrencyDepth, path + Constants.PATH_SEPARATOR + i);
        }
      }
      masterInfo._delete(path, true);
    }

    @Override
    public void run() {
      try {
        exec(this.depth, this.filesPerNode, this.concurrencyDepth, this.initPath);
      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
    }
  }

  @Test
  public void fsPerfTest() throws Exception {
    final int depth = 9;
    final int filesPerNode = 4;
    final int concurrencyDepth = 2;
    final String[] roots = { "/root1", "/root2", "/root3" };
    LOG.error("depth = " + depth);
    LOG.error("files per node = " + filesPerNode);
    LOG.error("concurrency depth = " + concurrencyDepth);
    LOG.error("number of roots = " + roots.length);

    ExecutorService executor;
    long sMs;
    executor = Executors.newCachedThreadPool();
    Future<?> futures[] = new Future<?>[roots.length];
    sMs = System.currentTimeMillis();
    for (int i = 0; i < roots.length; i ++) {
      futures[i] =
          executor.submit(new PerfTestCreateTree(depth, filesPerNode, concurrencyDepth, roots[i],
              mMasterInfo));
    }
    for (Future<?> f : futures) {
      f.get();
    }
    executor.shutdown();
    LOG.error("createFileTree: " + (System.currentTimeMillis() - sMs) + " ms");

    executor = Executors.newCachedThreadPool();
    sMs = System.currentTimeMillis();
    for (int i = 0; i < roots.length; i ++) {
      futures[i] =
          executor.submit(new PerfTestTraverseTree(depth, filesPerNode, concurrencyDepth,
              roots[i], mMasterInfo));
    }
    for (Future<?> f : futures) {
      f.get();
    }
    executor.shutdown();
    while (!executor.isTerminated())
      ;
    LOG.error("traverseFileTree: " + (System.currentTimeMillis() - sMs) + " ms");

    executor = Executors.newCachedThreadPool();
    sMs = System.currentTimeMillis();
    for (int i = 0; i < roots.length; i ++) {
      futures[i] =
          executor.submit(new PerfTestDeleteTree(depth, filesPerNode, concurrencyDepth, roots[i],
              mMasterInfo));
    }
    for (Future<?> f : futures) {
      f.get();
    }
    LOG.error("deleteFileTree: " + (System.currentTimeMillis() - sMs) + " ms");
  }
}
