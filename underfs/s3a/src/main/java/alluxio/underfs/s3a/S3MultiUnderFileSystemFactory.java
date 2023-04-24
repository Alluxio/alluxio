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

import alluxio.AlluxioURI;

import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;

import com.amazonaws.AmazonClientException;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.util.regex.Pattern;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link S3AUnderFileSystem}. It will ensure AWS credentials are present
 * before returning a client. The validity of the credentials is checked by the client.
 */
@ThreadSafe
public class S3MultiUnderFileSystemFactory implements UnderFileSystemFactory {

  public static final Pattern HEADER_S3_MULTI = Pattern.compile("^s3[0-9]://.*");

  /**
   * Constructs a new {@link S3MultiUnderFileSystemFactory}.
   */
  public S3MultiUnderFileSystemFactory() {}

  @Override
  public UnderFileSystem create(String path, UnderFileSystemConfiguration conf) {
    Preconditions.checkNotNull(path, "Unable to create UnderFileSystem instance:"
        + " URI path should not be null");

    try {
      return S3MultiUnderFileSystem.createInstance(new AlluxioURI(path), conf);
    } catch (AmazonClientException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean supportsPath(String path) {
    return path != null
        && (HEADER_S3_MULTI.matcher(path).matches());
  }
}
