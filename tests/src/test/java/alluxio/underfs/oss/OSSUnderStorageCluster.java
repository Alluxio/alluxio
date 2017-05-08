/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.oss;

import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemCluster;
import alluxio.underfs.options.DeleteOptions;
import alluxio.util.io.PathUtils;

import java.io.IOException;
import java.util.UUID;

public class OSSUnderStorageCluster extends UnderFileSystemCluster {

  private static final String INTEGRATION_OSS_BUCKET = "ossBucket";

  private String mOSSBucket;

  public OSSUnderStorageCluster(String baseDir) {
    super(baseDir);
    mOSSBucket = System.getProperty(INTEGRATION_OSS_BUCKET);
    mBaseDir = PathUtils.concatPath(mOSSBucket, UUID.randomUUID());
  }

  @Override
  public void cleanup() throws IOException {
    UnderFileSystem ufs = UnderFileSystem.Factory.create(mBaseDir);
    ufs.deleteDirectory(mBaseDir, DeleteOptions.defaults().setRecursive(true));
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
