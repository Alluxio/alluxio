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

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;
import tachyon.underfs.UnderFileSystemCluster;

/**
 * This class will use Amazon S3 as the backing store. The integration properties should be
 * specified in the module's pom file. Each instance of the cluster will run with a separate base
 * directory (user prefix + uuid). Each test will attempt to clean up their test directories, but
 * in cases of complete failure (ie. jvm crashed) the directory will need to be cleaned up through
 * manual means.
 */
public class S3UnderStorageCluster extends UnderFileSystemCluster {

  private static final String INTEGRATION_S3_ACCESS_KEY = "accessKey";
  private static final String INTEGRATION_S3_SECRET_KEY = "secretKey";
  private static final String INTEGRATION_S3_BUCKET = "s3Bucket";

  private String mS3Bucket;

  public S3UnderStorageCluster(String baseDir, TachyonConf tachyonConf) {
    super(baseDir, tachyonConf);
    String awsAccessKey = System.getProperty(INTEGRATION_S3_ACCESS_KEY);
    String awsSecretKey = System.getProperty(INTEGRATION_S3_SECRET_KEY);
    System.setProperty(Constants.S3_ACCESS_KEY, awsAccessKey);
    System.setProperty(Constants.S3_SECRET_KEY, awsSecretKey);
    mS3Bucket = System.getProperty(INTEGRATION_S3_BUCKET);
    mBaseDir = mS3Bucket + UUID.randomUUID();
  }

  @Override
  public void cleanup() throws IOException {
    String oldDir = mBaseDir;
    mBaseDir = mS3Bucket + UUID.randomUUID();
    UnderFileSystem ufs = UnderFileSystem.get(mBaseDir, mTachyonConf);
    ufs.delete(oldDir, true);
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
