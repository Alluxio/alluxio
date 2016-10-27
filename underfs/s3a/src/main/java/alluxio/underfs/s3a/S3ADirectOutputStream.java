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

import alluxio.util.io.PathUtils;

import com.amazonaws.services.s3.transfer.TransferManager;

import java.io.IOException;

/**
 * A stream for writing a file into S3. Unlike {@link S3AOutputStream}, this class directly
 * uploads the file to the final S3 path instead of a temporary Alluxio generated S3 path.
 */
public class S3ADirectOutputStream extends S3AOutputStream {
  /**
   * Constructs a new stream for writing a file.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param manager the transfer manager to upload the file with
   * @throws IOException when a non-Alluxio related error occurs
   */
  public S3ADirectOutputStream(String bucketName, String key, TransferManager manager)
      throws IOException {
    super(bucketName, key, manager);
  }

  @Override
  public String getUploadPath() {
    return PathUtils.getPermanentFileName(super.getUploadPath());
  }
}
