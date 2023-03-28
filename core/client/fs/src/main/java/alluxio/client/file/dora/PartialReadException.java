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

package alluxio.client.file.dora;

import java.io.IOException;

/**
 * Exception when a positioned read operation was incomplete due to an underlying exception
 * occurred. The number of bytes that was requested by the caller, and that was actually read
 * by the reader can be obtained. The underlying cause can be obtained by
 * {@link Exception#getCause()}.
 */
public class PartialReadException extends IOException {
  private final int mBytesRead;
  private final int mBytesWanted;

  /**
   * Constructor.
   *
   * @param bytesWanted number of bytes requested by the caller
   * @param bytesRead number of bytes actually read so far when the exception occurs
   * @param cause the cause
   */
  public PartialReadException(int bytesWanted, int bytesRead, Throwable cause) {
    super(String.format("Incomplete read due to exception, %d requested, %d actually read",
        bytesWanted, bytesRead),
        cause);
    mBytesRead = bytesRead;
    mBytesWanted = bytesWanted;
  }

  /**
   * @return number of bytes read
   */
  public int getBytesRead() {
    return mBytesRead;
  }

  /**
   * @return number of bytes requested by the caller
   */
  public int getBytesWanted() {
    return mBytesWanted;
  }
}
