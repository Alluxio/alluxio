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

import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;
import tachyon.underfs.UnderFileSystemCluster;

/**
 * This class will use Amazon S3 as the backing store.
 */
public class S3UnderStorageCluster extends UnderFileSystemCluster {

  private final String awsAccessKey = "";
  private final String awsSecretKey = "";

  public S3UnderStorageCluster(String baseDir, TachyonConf tachyonConf) {
    super(baseDir, tachyonConf);
    System.setProperty("fs.s3n.awsAccessKeyId", awsAccessKey);
    System.setProperty("fs.s3n.awsSecretAccessKey", awsSecretKey);
    mBaseDir = "s3n://calvin-s3-test/testdir" + Math.random() * 100;
  }

  @Override
  public void cleanup() throws IOException {
    String oldDir = mBaseDir;
    mBaseDir = "s3n://calvin-s3-test/testdir" + Math.random() * 100;
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
  public void shutdown() throws IOException {
  }

  @Override
  public void start() throws IOException {}
}
