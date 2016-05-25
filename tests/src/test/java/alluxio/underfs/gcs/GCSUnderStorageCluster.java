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

package alluxio.underfs.gcs;

import alluxio.Configuration;
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
 * This class will use Google Cloud Storage as the backing store. The integration properties should
 * be specified in the module's pom file. Each instance of the cluster will run with a separate base
 * directory (user prefix + uuid). Each test will attempt to clean up their test directories, but
 * in cases of complete failure (ie. jvm crashed) the directory will need to be cleaned up through
 * manual means.
 */
public class GCSUnderStorageCluster extends UnderFileSystemCluster {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final String INTEGRATION_GCS_BUCKET = "gcsBucket";

  private boolean mStarted;
  private String mGCSBucket;

  public GCSUnderStorageCluster(String baseDir, Configuration configuration) {
    super(baseDir, configuration);
    mGCSBucket = PathUtils.concatPath(System.getProperty(INTEGRATION_GCS_BUCKET),
        UUID.randomUUID());
    Preconditions.checkState(mGCSBucket != null && mGCSBucket != "",
        PreconditionMessage.GCS_BUCKET_MUST_BE_SET.format(INTEGRATION_GCS_BUCKET));
    mBaseDir = PathUtils.concatPath(mGCSBucket, UUID.randomUUID());
    mStarted = false;
  }

  @Override
  public void cleanup() throws IOException {
    mBaseDir = PathUtils.concatPath(mGCSBucket, UUID.randomUUID());
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
    LOG.info("Shutting down GCS testing cluster, deleting bucket contents in: " + mGCSBucket);
    UnderFileSystem ufs = UnderFileSystem.get(mGCSBucket, mConfiguration);
    ufs.delete(mGCSBucket, true);
  }

  @Override
  public void start() throws IOException {
    mStarted = true;
  }
}
