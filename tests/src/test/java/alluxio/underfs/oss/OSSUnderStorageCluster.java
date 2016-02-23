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

package alluxio.underfs.oss;

import alluxio.Configuration;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemCluster;
import alluxio.util.io.PathUtils;

import java.io.IOException;
import java.util.UUID;

public class OSSUnderStorageCluster extends UnderFileSystemCluster {

  private static final String INTEGRATION_OSS_BUCKET = "ossBucket";

  private String mOSSBucket;

  public OSSUnderStorageCluster(String baseDir, Configuration configuration) {
    super(baseDir, configuration);
    mOSSBucket = System.getProperty(INTEGRATION_OSS_BUCKET);
    mBaseDir = PathUtils.concatPath(mOSSBucket, UUID.randomUUID());
  }

  @Override
  public void cleanup() throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(mBaseDir, mConfiguration);
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
