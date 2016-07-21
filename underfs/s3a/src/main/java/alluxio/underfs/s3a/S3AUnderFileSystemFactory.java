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
import alluxio.Constants;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemFactory;

import com.amazonaws.AmazonClientException;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link S3AUnderFileSystem}. It will ensure AWS credentials are present
 * before returning a client. The validity of the credentials is checked by the client.
 */
@ThreadSafe
public class S3AUnderFileSystemFactory implements UnderFileSystemFactory {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Constructs a new {@link S3AUnderFileSystemFactory}.
   */
  public S3AUnderFileSystemFactory() {}

  @Override
  public UnderFileSystem create(String path, Object unusedConf) {
    Preconditions.checkNotNull(path);

    try {
      return new S3AUnderFileSystem(new AlluxioURI(path));
    } catch (AmazonClientException e) {
      LOG.error("Failed to create S3AUnderFileSystem.", e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean supportsPath(String path) {
    return path != null && path.startsWith(Constants.HEADER_S3A);
  }
}
