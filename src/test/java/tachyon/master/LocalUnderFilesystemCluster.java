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

import java.io.File;
import java.io.IOException;

import tachyon.util.CommonUtils;

public class LocalUnderFilesystemCluster {
  static private final String INTEGRATION_UFS_PROFILE_KEY = "ufs";

  protected String mBaseDir;

  public LocalUnderFilesystemCluster(String baseDir) {
    this.mBaseDir = baseDir;
  }

  public static LocalUnderFilesystemCluster getLocalUnderFilesystemCluster(String baseDir) {
    String ufsClz = System.getProperty(INTEGRATION_UFS_PROFILE_KEY);

    if (ufsClz != null && !ufsClz.equals("")) {
      try {
        LocalUnderFilesystemCluster ufsCluster = (LocalUnderFilesystemCluster) Class
            .forName(ufsClz).getConstructor(String.class).newInstance(baseDir);
        return ufsCluster;
      } catch (Exception e) {
        System.out.println("Failed to initialize ufsCluster for integration test");
        CommonUtils.runtimeException(e);
      }
    }
    return new LocalUnderFilesystemCluster(baseDir);
  }

  public void start() throws IOException {
  }

  public void shutdown() throws IOException {
  }

  public boolean isStarted() {
    return true;
  }

  public String getUnderFilesystemAddress() {
    return new File(this.mBaseDir).getAbsolutePath();
  }
}
