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
package tachyon;

import java.io.File;
import java.io.IOException;

import tachyon.conf.TachyonConf;

/**
 * A dummy filesystem cluster for testing UnderFileSystemDummy.
 */
public class DummyFilesystemCluster extends UnderFileSystemCluster {

  private boolean mIsStarted = false;

  public DummyFilesystemCluster(String dfsBaseDirs, TachyonConf tachyonConf) {
    super(dfsBaseDirs, tachyonConf);
  }

  public static boolean mkdirs(String path, TachyonConf tachyonConf) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path, tachyonConf);
    return ufs.mkdirs(path, true);
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

  @Override
  public String getUnderFilesystemAddress() {
    return new File(mBaseDir).getAbsolutePath();
  }

  @Override
  public boolean isStarted() {
    return mIsStarted;
  }

  @Override
  public void shutdown() throws IOException {
    if (mIsStarted) {
      mIsStarted = false;
    }
  }

  @Override
  public void start() throws IOException {
    if (!mIsStarted) {
      if (!mkdirs(mBaseDir, mTachyonConf)) {
        throw new IOException("Failed to make folder: " + mBaseDir);
      }
      mIsStarted = true;
    }
  }
}
