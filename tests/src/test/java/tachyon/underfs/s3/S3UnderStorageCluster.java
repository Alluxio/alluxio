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

package tachyon.underfs.s3;

import java.io.IOException;
import java.util.UUID;

import com.google.common.base.Preconditions;

import tachyon.conf.TachyonConf;
import tachyon.exception.PreconditionMessage;
import tachyon.underfs.UnderFileSystem;
import tachyon.underfs.UnderFileSystemCluster;
import tachyon.util.io.PathUtils;

/**
 * This class will use Amazon S3 as the backing store. The integration properties should be
 * specified in the module's pom file. Each instance of the cluster will run with a separate base
 * directory (user prefix + uuid). Each test will attempt to clean up their test directories, but
 * in cases of complete failure (ie. jvm crashed) the directory will need to be cleaned up through
 * manual means.
 */
public class S3UnderStorageCluster extends UnderFileSystemCluster {

  private static final String INTEGRATION_S3_BUCKET = "s3Bucket";

  private String mS3Bucket;

  public S3UnderStorageCluster(String baseDir, TachyonConf tachyonConf) {
    super(baseDir, tachyonConf);
    mS3Bucket = System.getProperty(INTEGRATION_S3_BUCKET);
    Preconditions.checkState(mS3Bucket != null && mS3Bucket != "",
        PreconditionMessage.S3_BUCKET_MUST_BE_SET, INTEGRATION_S3_BUCKET);
    mBaseDir = PathUtils.concatPath(mS3Bucket, UUID.randomUUID());
  }

  @Override
  public void cleanup() throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(mBaseDir, mTachyonConf);
    ufs.delete(mBaseDir, true);
    mBaseDir = PathUtils.concatPath(mS3Bucket, UUID.randomUUID());
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
  public void shutdown() throws IOException {}

  @Override
  public void start() throws IOException {}
}
