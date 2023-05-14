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

import alluxio.file.ReadTargetBuffer;
import alluxio.underfs.ObjectPositionReader;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.IOException;
import java.io.InputStream;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Implementation of {@link ObjectPositionReader} that reads from S3A object store.
 */
@ThreadSafe
public class S3APositionReader extends ObjectPositionReader {

  /**
   * Client for operations with s3.
   */
  protected AmazonS3 mClient;

  /**
   * S3 object.
   */
  protected S3Object mObject;

  /**
   * @param client     the amazon s3 client
   * @param bucketName the bucket name
   * @param path       the file path
   * @param fileLength the file length
   */
  public S3APositionReader(AmazonS3 client, String bucketName, String path, long fileLength) {
    // TODO(lu) path needs to be transform to not include bucket
    super(bucketName, path, fileLength);
    mClient = client;
  }

  @Override
  protected InputStream getObjectInputStream(
      long position, ReadTargetBuffer buffer,
      int bytesToRead, String errorMessage) throws IOException {
    try {
      GetObjectRequest getObjectRequest = new GetObjectRequest(mBucketName, mPath);
      getObjectRequest.setRange(position, position + bytesToRead - 1);
      mObject = mClient.getObject(getObjectRequest);
    } catch (AmazonS3Exception e) {
      if (e.getStatusCode() == 416) {
        // InvalidRange exception when mPos >= file length
        throw AlluxioS3Exception.from(String.format("Underlying file may be changed. "
            + "Expected file length is %s but read %s bytes "
            + "from position %s is out of range", mFileLength, bytesToRead, position), e);
      }
      throw AlluxioS3Exception.from(errorMessage, e);
    }

    return mObject.getObjectContent();
  }
}
