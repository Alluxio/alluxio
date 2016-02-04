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

package alluxio.underfs.oss;

import java.io.IOException;
import java.util.UUID;

import alluxio.conf.TachyonConf;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemCluster;
import alluxio.util.io.PathUtils;

public class OSSUnderStorageCluster extends UnderFileSystemCluster {

  private static final String INTEGRATION_OSS_BUCKET = "ossBucket";

  private String mOSSBucket;

  public OSSUnderStorageCluster(String baseDir, TachyonConf tachyonConf) {
    super(baseDir, tachyonConf);
    mOSSBucket = System.getProperty(INTEGRATION_OSS_BUCKET);
    mBaseDir = PathUtils.concatPath(mOSSBucket, UUID.randomUUID());
  }

  @Override
  public void cleanup() throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(mBaseDir, mTachyonConf);
    ufs.delete(mBaseDir, true);
    mBaseDir = PathUtils.concatPath(mOSSBucket, UUID.randomUUID());
  }

  @Override
  public String getUnderFilesystemAddress() {
    return mBaseDir;
  }

  @Override
  public boolean isStarted() {
    return true;
  }

  @Override
  public void registerJVMOnExistHook() throws IOException {
    super.registerJVMOnExistHook();
  }

  @Override
  public void shutdown() throws IOException {
  }

  @Override
  public void start() throws IOException {
  }
}
