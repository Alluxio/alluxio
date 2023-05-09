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

import alluxio.PositionReader;
import alluxio.file.ReadTargetBuffer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.IOException;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Implementation of {@link PositionReader} that reads from S3A object store.
 */
@ThreadSafe
public class S3APositionReader implements PositionReader {
  private final String mPath;
  private final long mFileLength;
  /** Client for operations with s3. */
  protected AmazonS3 mClient;
  /** Name of the bucket the object resides in. */
  protected final String mBucketName;

  /**
   * @param client the amazon s3 client
   * @param bucketName the bucket name
   * @param path the file path
   * @param fileLength the file length
   */
  public S3APositionReader(AmazonS3 client, String bucketName, String path, long fileLength) {
    mClient = client;
    mBucketName = bucketName;
    // TODO(lu) path needs to be transform to not include bucket
    mPath = path;
    mFileLength = fileLength;
  }

  @Override
  public int readInternal(long position, ReadTargetBuffer buffer, int length)
      throws IOException {
    if (position >= mFileLength) { // at end of file
      return -1;
    }
    S3Object object;
    int bytesToRead = (int) Math.min(mFileLength - position, length);
    try {
      // Range check approach: set range (inclusive start, inclusive end)
      // start: should be < file length, error out otherwise
      //        e.g. error out when start == 0 && fileLength == 0
      //        start < 0, read all
      // end: if start > end, read all
      //      if start <= end < file length, read from start to end
      //      if end >= file length, read from start to file length - 1
      GetObjectRequest getObjectRequest = new GetObjectRequest(mBucketName, mPath);
      getObjectRequest.setRange(position, position + bytesToRead - 1);
      object = mClient.getObject(getObjectRequest);
    } catch (AmazonS3Exception e) {
      if (e.getStatusCode() == 416) {
        // InvalidRange exception when mPos >= file length
        throw AlluxioS3Exception.from(String
            .format("Underlying file may be changed. "
                + "Expected file length is %s but read %s bytes from position %s is out of range",
                mFileLength, bytesToRead, position), e);
      }
      throw AlluxioS3Exception.from(String
          .format("Failed to get object: %s bucket: %s", mPath, mBucketName), e);
    }
    int totalRead;
    try (S3ObjectInputStream in = object.getObjectContent()) {
      totalRead = readDataInternal(in, buffer, bytesToRead);
    }
    return totalRead;
  }
}
