/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client.file;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.ClientContext;
import tachyon.client.file.options.GetInfoOptions;
import tachyon.client.file.options.OpenOptions;
import tachyon.exception.TachyonException;
import tachyon.exception.TachyonExceptionType;
import tachyon.util.CommonUtils;

/**
 * Collection of utility methods to handle with {@link TachyonFileSystem} related objects
 */
public final class TachyonFileSystemUtils {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  // prevent instantiation
  private TachyonFileSystemUtils() {}

  /**
   * Shortcut for {@code waitCompleted(tfs, uri, -1, TimeUnit.MILLISECONDS)}, i.e., wait for an
   * indefinite amount of time. Note that if a file is never completed, the thread will block
   * forever, so use with care.
   *
   * @see #waitCompleted(TachyonFileSystemCore, TachyonURI, long, TimeUnit)
   * @param tfs a {@link TachyonFileSystemCore} instance
   * @param uri the URI of the file on which the thread should wait
   * @return true if the file is complete when this method returns and false
   * if the method timed out before the file was complete.
   *
   * @throws IOException in case there are problems contacting the Tachyonmaster
   * for the file status
   * @throws TachyonException if a Tachyon Exception occurs
   * @throws InterruptedException if the thread receives an interrupt while
   * waiting for file completion
   */
  public static boolean waitCompleted(TachyonFileSystemCore tfs, TachyonURI uri)
      throws IOException, TachyonException, InterruptedException {
    return TachyonFileSystemUtils.waitCompleted(tfs, uri, -1, TimeUnit.MILLISECONDS);
  }

  /**
   * Wait for a file to be marked as completed.
   *
   * The calling thread will block for <i>at most</i> {@code timeout} time units (as specified via
   * {@code tunit} or until the TachyonFile is reported as complete by the master. The method will
   * return the last known completion status of the file (hence, false only if the method has timed
   * out). A zero value on the {@code timeout} parameter will make the calling thread check once and
   * return; a negative value will make it block indefinitely. Note that, in this last case, if a
   * file is never completed, the thread will block forever, so use with care.
   *
   * Note that the file whose uri is specified, might not exist at the moment this method this call.
   * The method will deliberately block anyway for the specified amount of time, waiting for the
   * file to be created and eventually completed. Note also that the file might be moved or deleted
   * while it is waited upon. In such cases the method will throw the a {@link TachyonException}
   * with the appropriate {@link tachyon.exception.TachyonExceptionType}
   *
   * <i>IMPLEMENTATION NOTES</i> This method is implemented
   * by periodically polling the master about the file status. The
   * polling period is controlled by the {@link Constants#USER_FILE_WAITCOMPLETED_POLL_MS}
   * java property and defaults to a generous 1 second.
   *
   * @param tfs an instance of {@link TachyonFileSystemCore}
   * @param uri the URI of the file whose completion status is to be watied for.
   * @param timeout maximum time the calling thread should be blocked on this call.
   * @param tunit the @{link TimeUnit} instance describing the {@code timeout} parameter
   * @return true if the file is complete when this method returns and false if the method timed out
   *         before the file was complete.
   *
   * @throws IOException in case there are problems contacting the Tachyonmaster for the file status
   * @throws TachyonException if a Tachyon Exception occurs
   * @throws InterruptedException if the thread receives an interrupt while waiting for file
   *         completion
   */
  public static boolean waitCompleted(final TachyonFileSystemCore tfs, final TachyonURI uri,
      final long timeout, final TimeUnit tunit) throws IOException, TachyonException,
      InterruptedException {

    final long deadline = System.currentTimeMillis() + tunit.toMillis(timeout);
    final long pollPeriod =
        ClientContext.getConf().getLong(Constants.USER_FILE_WAITCOMPLETED_POLL_MS);
    TachyonFile file = null;
    boolean completed = false;
    long timeleft = deadline - System.currentTimeMillis();
    long toSleep = 0;

    while (!completed && (timeout <= 0 || timeleft > 0)) {

      if (file == null) {
        file = tfs.openIfExists(uri, OpenOptions.defaults());
        if (file == null) {
          LOG.debug("The file {} being waited upon does not exist yet. Waiting for it to be "
              + "created.", uri);
        }
      }

      if (file != null) {
        completed = tfs.getInfo(file, GetInfoOptions.defaults()).isCompleted;
      }

      if (timeout == 0) {
        return completed;
      } else if (!completed) {
        if (timeout < 0 || timeleft > pollPeriod) {
          toSleep = pollPeriod;
        } else {
          toSleep = timeleft;
        }

        CommonUtils.sleepMs(LOG, toSleep, true);
        timeleft = deadline - System.currentTimeMillis();
      }
    }

    return completed;
  }
}
