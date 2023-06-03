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

package alluxio.client.file.dora.netty;

import alluxio.exception.status.AlluxioStatusException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Exception when a positioned read operation was incomplete due to an underlying exception
 * occurred. The number of bytes that was requested by the caller, and that was actually read
 * by the reader can be obtained. The underlying cause can be obtained by
 * {@link Exception#getCause()}.
 */
public class PartialReadException extends IOException {
  private static final Logger LOG = LoggerFactory.getLogger(PartialReadException.class);
  private final int mBytesRead;
  private final int mBytesWanted;
  private final CauseType mCauseType;

  /**
   * Constructor.
   *
   * @param bytesWanted number of bytes requested by the caller
   * @param bytesRead number of bytes actually read so far when the exception occurs
   * @param causeType type of cause
   * @param cause the cause
   */
  public PartialReadException(int bytesWanted, int bytesRead, CauseType causeType,
      Throwable cause) {
    super(String.format("Incomplete read due to exception, %d requested, %d actually read",
        bytesWanted, bytesRead),
        cause);
    if (!causeType.getType().isInstance(cause)) {
      LOG.error("Failed to read {}, actual read {}", bytesWanted, bytesRead, cause);
      throw new IllegalArgumentException(String.format(
          "Cause type %s is not the instance of cause %s", causeType.getType(), cause));
    }
    mBytesRead = bytesRead;
    mBytesWanted = bytesWanted;
    mCauseType = causeType;
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

  /**
   * @return the type of the cause
   */
  public CauseType getCauseType() {
    return mCauseType;
  }

  /**
   * Possible causes of {@link PartialReadException}.
   * <ol>
   * <li>InterruptedException: client is waiting for the next packet but the thread is
   *     interrupted</li>
   * <li>TimeoutException: no new packets have been delivered in a long time, but the channel is
   *     considered still alive </li>
   * <li>AlluxioStatusException: server side error, channel is intact</li>
   * <li>Throwable (unknown exception): the channel is closed, or an unhandled exception in
   *     channel handler, etc., the channel is probably in a bad state to do any further I/O</li>
   * <li>IOException: exception when trying to write data into the output channel</li>
   * <li>EOFException: early eof, i.e. fewer bytes actually than expected</li>
   * </ol>
   * Except for 5, all the exceptions can be tried within the same channel.
   *
   */
  public enum CauseType {
    INTERRUPT(InterruptedException.class),
    TIMEOUT(TimeoutException.class),
    SERVER_ERROR(AlluxioStatusException.class),
    TRANSPORT_ERROR(Throwable.class),
    OUTPUT(IOException.class),
    EARLY_EOF(EOFException.class);

    private final Class<? extends Throwable> mCauseType;

    private CauseType(Class<? extends Throwable> type) {
      mCauseType = type;
    }

    /**
     * @return the exception type
     */
    public Class<? extends Throwable> getType() {
      return mCauseType;
    }
  }
}
