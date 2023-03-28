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
  private static final String MESSAGE = "Incomplete read due to exception";
  private final int mBytesRead;
  private final int mBytesWanted;

  /**
   * Constructor.
   *
   * @param bytesWanted
   * @param bytesRead
   * @param cause
   */
  public PartialReadException(int bytesWanted, int bytesRead, Throwable cause) {
    super(MESSAGE, cause);
    mBytesRead = bytesRead;
    mBytesWanted = bytesWanted;
  }

  /**
   * @return
   */
  public int getBytesRead() {
    return mBytesRead;
  }

  /**
   * @return
   */
  public int getBytesWanted() {
    return mBytesWanted;
  }
}
