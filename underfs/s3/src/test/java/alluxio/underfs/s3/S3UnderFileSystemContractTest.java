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

import alluxio.exception.PreconditionMessage;
import alluxio.underfs.AbstractUnderFileSystemContractTest;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;

import com.google.common.base.Preconditions;

/**
 * This UFS contract test will use Amazon S3 as the backing store.
 */
public final class S3UnderFileSystemContractTest extends AbstractUnderFileSystemContractTest {
  private static final String INTEGRATION_S3_BUCKET = "s3Bucket";
  private String mS3Bucket;

  public S3UnderFileSystemContractTest() {
    String s3Bucket = System.getProperty(INTEGRATION_S3_BUCKET);
    Preconditions.checkState(s3Bucket != null,
        PreconditionMessage.S3_BUCKET_MUST_BE_SET.toString(), INTEGRATION_S3_BUCKET);
    Preconditions.checkState(new S3UnderFileSystemFactory().supportsPath(s3Bucket));
    mS3Bucket = s3Bucket;
  }

  @Override
  public UnderFileSystem createUfs(String path, UnderFileSystemConfiguration conf)
      throws Exception {
    return new S3UnderFileSystemFactory().create(path, conf);
  }

  @Override
  public String getUfsBaseDir() {
    return mS3Bucket;
  }
}
