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

package alluxio.underfs.s3a;

import alluxio.exception.PreconditionMessage;
import alluxio.underfs.AbstractUnderFileSystemContractTest;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;

import java.util.UUID;

/**
 * This UFS contract test will use Amazon S3 as the backing store.
 */
public final class S3AUnderFileSystemContractTest extends AbstractUnderFileSystemContractTest {
  private static final String INTEGRATION_S3_BUCKET = "s3Bucket";
  private String mS3Bucket;

  @Override
  public UnderFileSystem createUfs(String path, UnderFileSystemConfiguration conf)
      throws Exception {
    return new S3AUnderFileSystemFactory().create(path, conf);
  }

  @Override
  public String getUfsBaseDir() {
    mS3Bucket = PathUtils.concatPath(System.getProperty(INTEGRATION_S3_BUCKET), UUID.randomUUID());
    Preconditions.checkState(mS3Bucket != null && !mS3Bucket.equals(""),
        PreconditionMessage.S3_BUCKET_MUST_BE_SET.toString(), INTEGRATION_S3_BUCKET);
    return mS3Bucket;
  }
}
