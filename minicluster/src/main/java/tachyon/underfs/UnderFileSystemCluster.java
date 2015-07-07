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

package tachyon.underfs;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Throwables;

import tachyon.conf.TachyonConf;
import tachyon.LocalFilesystemCluster;
import tachyon.TachyonURI;
import tachyon.util.CommonUtils;

public abstract class UnderFileSystemCluster {
  class ShutdownHook extends Thread {
    UnderFileSystemCluster mUFSCluster = null;

    public ShutdownHook(UnderFileSystemCluster ufsCluster) {
      mUFSCluster = ufsCluster;
    }

    @Override
    public void run() {
      if (mUFSCluster != null) {
        try {
          mUFSCluster.shutdown();
        } catch (IOException e) {
          System.out.println("Failed to shutdown underfs cluster: " + e);
        }
      }
    }
  }

  private static final String INTEGRATION_UFS_PROFILE_KEY = "ufs";

  private static String sUfsClz;

  /**
   * To start the underfs test bed and register the shutdown hook
   *
   * @throws IOException
   * @throws InterruptedException
   */
  public static synchronized UnderFileSystemCluster get(String baseDir, TachyonConf tachyonConf)
      throws IOException {
    if (null == sUnderFSCluster) {
      sUnderFSCluster = getUnderFilesystemCluster(baseDir, tachyonConf);
    }

    if (!sUnderFSCluster.isStarted()) {
      sUnderFSCluster.start();
      sUnderFSCluster.registerJVMOnExistHook();
    }

    return sUnderFSCluster;
  }

  public static UnderFileSystemCluster getUnderFilesystemCluster(String baseDir,
      TachyonConf tachyonConf) {
    sUfsClz = System.getProperty(INTEGRATION_UFS_PROFILE_KEY);

    if (!StringUtils.isEmpty(sUfsClz)) {
      try {
        UnderFileSystemCluster ufsCluster =
            (UnderFileSystemCluster) Class.forName(sUfsClz).getConstructor(String.class,
                TachyonConf.class).newInstance(baseDir, tachyonConf);
        System.out.println("Initialized under file system testing cluster of type "
            + ufsCluster.getClass().getCanonicalName() + " for integration testing");
        return ufsCluster;
      } catch (Throwable e) {
        System.err.println("Failed to initialize the ufsCluster of " + sUfsClz
            + " for integration test.");
        throw Throwables.propagate(e);
      }
    }
    System.out.println("Using default LocalFilesystemCluster for integration testing");
    return new LocalFilesystemCluster(baseDir, tachyonConf);
  }

  protected String mBaseDir;

  private static UnderFileSystemCluster sUnderFSCluster = null;

  protected final TachyonConf mTachyonConf;

  /**
   * This method is only used by the {@link tachyon.client.FileOutStreamIntegrationTest} unit-test
   *
   * @return true if reads on end of file return negative otherwise false
   */
  public static boolean readEOFReturnsNegative() {
    // TODO Should be dynamically determined - may need additional method on UnderFileSystem
    return null != sUfsClz && sUfsClz.equals("tachyon.underfs.hdfs.LocalMiniDFSCluster");
  }

  public UnderFileSystemCluster(String baseDir, TachyonConf tachyonConf) {
    mBaseDir = baseDir;
    mTachyonConf = tachyonConf;
  }

  /**
   * To clean up the test environment over underfs cluster system, so that we can re-use the running
   * system for the next test round instead of turning on/off it from time to time. This function is
   * expected to be called either before or after each test case which avoids certain overhead from
   * the bootstrap.
   *
   * @throws IOException
   */
  public void cleanup() throws IOException {
    if (isStarted()) {
      String path = getUnderFilesystemAddress() + TachyonURI.SEPARATOR;
      UnderFileSystem ufs = UnderFileSystem.get(path, mTachyonConf);
      for (String p : ufs.list(path)) {
        ufs.delete(CommonUtils.concatPath(path, p), true);
      }
    }
  }

  public abstract String getUnderFilesystemAddress();

  /**
   * Check if the cluster started.
   *
   * @return
   */
  public abstract boolean isStarted();

  /**
   * Add a shutdown hook. The {@link #shutdown} phase will be automatically called while the process
   * exists.
   *
   * @throws InterruptedException
   */
  public void registerJVMOnExistHook() throws IOException {
    Runtime.getRuntime().addShutdownHook(new ShutdownHook(this));
  }

  /**
   * To stop the underfs cluster system
   *
   * @throws IOException
   */
  public abstract void shutdown() throws IOException;

  /**
   * To start the underfs cluster system
   *
   * @throws IOException
   */
  public abstract void start() throws IOException;
}
