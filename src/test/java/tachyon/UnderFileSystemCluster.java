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
package tachyon;

import java.io.IOException;

import tachyon.util.CommonUtils;

public abstract class UnderFileSystemCluster {
  private final static String INTEGRATION_UFS_PROFILE_KEY = "ufs";

  private static String mUfsClz;

  protected String mBaseDir;

  public UnderFileSystemCluster(String baseDir) {
    mBaseDir = baseDir;
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

  /**
   * This method is only for unit-test {@link tachyon.client.FileOutStreamTest} temporally
   * 
   * @return
   */
  public static boolean isUFSHDFS() {
    return mUfsClz.equals("tachyon.LocalMiniDFSCluster");
  }

  abstract public void start() throws IOException;

  abstract public void shutdown() throws IOException;

  abstract public boolean isStarted();

  abstract public String getUnderFilesystemAddress();
}
