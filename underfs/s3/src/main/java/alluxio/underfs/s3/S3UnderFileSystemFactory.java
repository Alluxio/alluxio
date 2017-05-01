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

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.jets3t.service.ServiceException;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link S3UnderFileSystem}. It will ensure AWS credentials are present before
 * returning a client. The validity of the credentials is checked by the client.
 */
@ThreadSafe
public class S3UnderFileSystemFactory implements UnderFileSystemFactory {

  /**
   * Constructs a new {@link S3UnderFileSystemFactory}.
   */
  public S3UnderFileSystemFactory() {}

  @Override
  public UnderFileSystem create(String path, UnderFileSystemConfiguration conf) {
    Preconditions.checkNotNull(path, "path");
    if (checkAWSCredentials(conf)) {
      try {
        return S3UnderFileSystem.createInstance(new AlluxioURI(path), conf);
      } catch (ServiceException e) {
        throw Throwables.propagate(e);
      }
    }

    String err = "AWS Credentials not available, cannot create S3 Under File System.";
    throw Throwables.propagate(new IOException(err));
  }

  @Override
  public boolean supportsPath(String path) {
    return path != null && path.startsWith(Constants.HEADER_S3N);
  }

  /**
   * @return true if both access and secret key are present, false otherwise
   */
  private boolean checkAWSCredentials(UnderFileSystemConfiguration conf) {
    return conf.containsKey(PropertyKey.S3N_ACCESS_KEY) && conf
        .containsKey(PropertyKey.S3N_SECRET_KEY);
  }
}
