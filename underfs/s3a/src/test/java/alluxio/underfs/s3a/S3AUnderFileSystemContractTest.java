/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0.
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

import com.google.common.base.Preconditions;
import org.junit.BeforeClass;

/**
 * This UFS contract test will use Amazon S3 as the backing store.
 */
public final class S3AUnderFileSystemContractTest extends AbstractUnderFileSystemContractTest {
  private static final String S3A_BUCKET_CONF = "testS3ABucket";
  private static final String S3A_BUCKET = System.getProperty(S3A_BUCKET_CONF);

  @BeforeClass
  public static void beforeClass() throws Exception {
    Preconditions.checkNotNull(S3A_BUCKET,
        PreconditionMessage.S3_BUCKET_MUST_BE_SET.toString(), S3A_BUCKET);
    Preconditions.checkState(new S3AUnderFileSystemFactory().supportsPath(S3A_BUCKET),
        "%s is not a valid S3 path", S3A_BUCKET);
  }

  @Override
  public UnderFileSystem createUfs(String path, UnderFileSystemConfiguration conf)
      throws Exception {
    return new S3AUnderFileSystemFactory().create(path, conf);
  }

  @Override
  public String getUfsBaseDir() {
    return S3A_BUCKET;
  }
}
