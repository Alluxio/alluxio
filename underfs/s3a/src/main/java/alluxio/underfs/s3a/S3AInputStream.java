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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.IOException;
import java.io.InputStream;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A wrapper around an {@link S3ObjectInputStream} which handles skips efficiently.
 */
@NotThreadSafe
public class S3AInputStream extends InputStream {
  /** Client for operations with s3. */
  private final AmazonS3 mClient;
  /** Name of the bucket the object resides in. */
  private final String mBucketName;
  /** The path of the object to read. */
  private final String mKey;

  /** The backing input stream from s3. */
  private S3ObjectInputStream mIn;
  /** The current position of the stream. */
  private long mPos;

  /**
   * Constructor for an input stream of an object in s3 using the aws-sdk implementation to read
   * the data. The stream will be positioned at the start of the file.
   *
   * @param bucketName the bucket the object resides in
   * @param key the path of the object to read
   * @param client the s3 client to use for operations
   */
  public S3AInputStream(String bucketName, String key, AmazonS3 client) {
    this(bucketName, key, client, 0L);
  }

  /**
   * Constructor for an input stream of an object in s3 using the aws-sdk implementation to read
   * the data. The stream will be positioned at the specified position.
   *
   * @param bucketName the bucket the object resides in
   * @param key the path of the object to read
   * @param client the s3 client to use for operations
   * @param position the position to begin reading from
   */
  public S3AInputStream(String bucketName, String key, AmazonS3 client, long position) {
    mBucketName = bucketName;
    mKey = key;
    mClient = client;
    mPos = position;
  }

  @Override
  public void close() {
    closeStream();
  }

  @Override
  public int read() throws IOException {
    if (mIn == null) {
      openStream();
    }
    int value = mIn.read();
    if (value != -1) { // valid data read
      mPos++;
    }
    return value;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int offset, int length) throws IOException {
    if (length == 0) {
      return 0;
    }
    if (mIn == null) {
      openStream();
    }
    int read = mIn.read(b, offset, length);
    if (read != -1) {
      mPos += read;
    }
    return read;
  }

  @Override
  public long skip(long n) {
    if (n <= 0) {
      return 0;
    }
    closeStream();
    mPos += n;
    openStream();
    return n;
  }

  /**
   * Opens a new stream at mPos if the wrapped stream mIn is null.
   */
  private void openStream() {
    if (mIn != null) { // stream is already open
      return;
    }
    GetObjectRequest getReq = new GetObjectRequest(mBucketName, mKey);
    // If the position is 0, setting range is redundant and causes an error if the file is 0 length
    if (mPos > 0) {
      getReq.setRange(mPos);
    }
    mIn = mClient.getObject(getReq).getObjectContent();
  }

  /**
   * Closes the current stream.
   */
  // TODO(calvin): Investigate if close instead of abort will bring performance benefits.
  private void closeStream() {
    if (mIn == null) {
      return;
    }
    mIn.abort();
    mIn = null;
  }
}
