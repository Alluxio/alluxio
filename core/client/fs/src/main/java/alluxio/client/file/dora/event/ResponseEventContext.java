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

package alluxio.client.file.dora.event;

import java.nio.channels.WritableByteChannel;

/**
 * Response event context.
 * This is used for recording the following information during
 * the period of processing a response event:
 * <li>the length to read</li>
 * <li>the number of bytes read</li>
 * <li>the related outbound channel</li>
 */
public class ResponseEventContext {

  private final int mLengthToRead;

  private int mBytesRead;

  private final WritableByteChannel mRelatedOutChannel;

  /**
   * Response event context.
   *
   * @param lengthToRead the length to read
   * @param bytesRead the number of bytes read
   * @param relatedOutChannel the related outbound channel
   */
  public ResponseEventContext(
      int lengthToRead,
      int bytesRead,
      WritableByteChannel relatedOutChannel) {
    mLengthToRead = lengthToRead;
    mBytesRead = bytesRead;
    mRelatedOutChannel = relatedOutChannel;
  }

  /**
   * Get the total length to read.
   *
   * @return length to read
   */
  public int getLengthToRead() {
    return mLengthToRead;
  }

  /**
   * Get the bytes read.
   * @return the bytes read
   */
  public int getBytesRead() {
    return mBytesRead;
  }

  /**
   * Get the related outbound channel.
   * @return the related outbound channel
   */
  public WritableByteChannel getRelatedOutChannel() {
    return mRelatedOutChannel;
  }

  /**
   * Increase the number of bytes read.
   * @param bytesRead
   */
  public void increaseBytesRead(int bytesRead) {
    mBytesRead += bytesRead;
  }
}
