/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon;

import java.io.IOException;

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

  private static String mUfsClz;

  /**
   * To start the underfs test bed and register the shutdown hook
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  public static synchronized UnderFileSystemCluster get(String baseDir) throws IOException {
    if (null == mUnderFSCluster) {
      mUnderFSCluster = getUnderFilesystemCluster(baseDir);
    }

    if (!mUnderFSCluster.isStarted()) {
      mUnderFSCluster.start();
      mUnderFSCluster.registerJVMOnExistHook();
    }

    return mUnderFSCluster;
  }

  public static UnderFileSystemCluster getUnderFilesystemCluster(String baseDir) {
    mUfsClz = System.getProperty(INTEGRATION_UFS_PROFILE_KEY);

    if (mUfsClz != null && !mUfsClz.equals("")) {
      try {
        UnderFileSystemCluster ufsCluster =
            (UnderFileSystemCluster) Class.forName(mUfsClz).getConstructor(String.class)
                .newInstance(baseDir);
        return ufsCluster;
      } catch (Exception e) {
        System.out.println("Failed to initialize the ufsCluster of " + mUfsClz
            + " for integration test.");
        CommonUtils.runtimeException(e);
      }
    }
    return new LocalFilesystemCluster(baseDir);
  }

  protected String mBaseDir;

  private static UnderFileSystemCluster mUnderFSCluster = null;

  /**
   * This method is only for unit-test {@link tachyon.client.FileOutStreamTest} temporally
   * 
   * @return
   */
  public static boolean isUFSHDFS() {
    return (null != mUfsClz) && (mUfsClz.equals("tachyon.LocalMiniDFSCluster"));
  }

  public UnderFileSystemCluster(String baseDir) {
    mBaseDir = baseDir;
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
      String path = getUnderFilesystemAddress() + Constants.PATH_SEPARATOR;
      UnderFileSystem ufs = UnderFileSystem.get(path);
      for (String p : ufs.list(path)) {
        ufs.delete(CommonUtils.concat(path, p), true);
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
   * Add a shutdown hook. The {@link #shutdown} phase will be automatically
   * called while the process exists.
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
