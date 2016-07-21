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

package alluxio.underfs.s3;

import alluxio.Constants;
import alluxio.exception.PreconditionMessage;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemCluster;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

/**
 * This class will use Amazon S3 as the backing store. The integration properties should be
 * specified in the module's pom file. Each instance of the cluster will run with a separate base
 * directory (user prefix + uuid). Each test will attempt to clean up their test directories, but
 * in cases of complete failure (ie. jvm crashed) the directory will need to be cleaned up through
 * manual means.
 */
public class S3UnderStorageCluster extends UnderFileSystemCluster {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final String INTEGRATION_S3_BUCKET = "s3Bucket";

  private boolean mStarted;
  private String mS3Bucket;

  public S3UnderStorageCluster(String baseDir) {
    super(baseDir);
    mS3Bucket = PathUtils.concatPath(System.getProperty(INTEGRATION_S3_BUCKET), UUID.randomUUID());
    Preconditions.checkState(mS3Bucket != null && mS3Bucket != "",
        PreconditionMessage.S3_BUCKET_MUST_BE_SET.toString(), INTEGRATION_S3_BUCKET);
    mBaseDir = PathUtils.concatPath(mS3Bucket, UUID.randomUUID());
    mStarted = false;
  }

  @Override
  public void cleanup() throws IOException {
    mBaseDir = PathUtils.concatPath(mS3Bucket, UUID.randomUUID());
  }

  @Override
  public String getUnderFilesystemAddress() {
    return mBaseDir;
  }

  @Override
  public boolean isStarted() {
    return mStarted;
  }

  @Override
  public void shutdown() throws IOException {
    LOG.info("Shutting down S3 testing cluster, deleting bucket contents in: " + mS3Bucket);
    UnderFileSystem ufs = UnderFileSystem.get(mS3Bucket);
    ufs.delete(mS3Bucket, true);
  }

  @Override
  public void start() throws IOException {
    mStarted = true;
  }
}
