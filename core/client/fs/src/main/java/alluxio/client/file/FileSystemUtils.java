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

package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.ScheduleAsyncPersistencePOptions;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Collection of utility methods to handle with {@link FileSystem} related objects.
 */
@ThreadSafe
public final class FileSystemUtils {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemUtils.class);

  // prevent instantiation
  private FileSystemUtils() {}

  /**
   * Shortcut for {@code waitCompleted(fs, uri, -1, TimeUnit.MILLISECONDS)}, i.e., wait for an
   * indefinite amount of time. Note that if a file is never completed, the thread will block
   * forever, so use with care.
   *
   * @param fs a {@link FileSystem} instance
   * @param uri the URI of the file on which the thread should wait
   * @return true if the file is complete when this method returns and false if the method timed out
   *         before the file was complete.
   * @throws InterruptedException if the thread receives an interrupt while waiting for file
   *         completion
   * @see #waitCompleted(FileSystem, AlluxioURI, long, TimeUnit)
   */
  public static boolean waitCompleted(FileSystem fs, AlluxioURI uri)
      throws IOException, AlluxioException, InterruptedException {
    return FileSystemUtils.waitCompleted(fs, uri, -1, TimeUnit.MILLISECONDS);
  }

  /**
   * Waits for a file to be marked as completed.
   * <p/>
   * The calling thread will block for <i>at most</i> {@code timeout} time units (as specified via
   * {@code tunit} or until the file is reported as complete by the master. The method will return
   * the last known completion status of the file (hence, false only if the method has timed out). A
   * zero value on the {@code timeout} parameter will make the calling thread check once and return;
   * a negative value will make it block indefinitely. Note that, in this last case, if a file is
   * never completed, the thread will block forever, so use with care.
   * <p/>
   * Note that the file whose uri is specified, might not exist at the moment this method this call.
   * The method will deliberately block anyway for the specified amount of time, waiting for the
   * file to be created and eventually completed. Note also that the file might be moved or deleted
   * while it is waited upon. In such cases the method will throw the a {@link AlluxioException}
   * <p/>
   * <i>IMPLEMENTATION NOTES</i> This method is implemented by periodically polling the master about
   * the file status. The polling period is controlled by the
   * {@link PropertyKey#USER_FILE_WAITCOMPLETED_POLL_MS} java property and defaults to a generous 1
   * second.
   *
   * @param fs an instance of {@link FileSystem}
   * @param uri the URI of the file whose completion status is to be watied for
   * @param timeout maximum time the calling thread should be blocked on this call
   * @param tunit the @{link TimeUnit} instance describing the {@code timeout} parameter
   * @return true if the file is complete when this method returns and false if the method timed out
   *         before the file was complete.
   * @throws InterruptedException if the thread receives an interrupt while waiting for file
   *         completion
   */
  public static boolean waitCompleted(final FileSystem fs, final AlluxioURI uri,
      final long timeout, final TimeUnit tunit)
      throws IOException, AlluxioException, InterruptedException {

    final long deadline = System.currentTimeMillis() + tunit.toMillis(timeout);
    final long fileWaitCompletedPollMs =
        fs.getConf().getMs(PropertyKey.USER_FILE_WAITCOMPLETED_POLL_MS);
    boolean completed = false;
    long timeleft = deadline - System.currentTimeMillis();

    while (!completed && (timeout <= 0 || timeleft > 0)) {

      if (!fs.exists(uri)) {
        LOG.debug("The file {} being waited upon does not exist yet. Waiting for it to be "
            + "created.", uri);
      } else {
        completed = fs.getStatus(uri).isCompleted();
      }

      if (timeout == 0) {
        return completed;
      } else if (!completed) {
        long toSleep;

        if (timeout < 0 || timeleft > fileWaitCompletedPollMs) {
          toSleep = fileWaitCompletedPollMs;
        } else {
          toSleep = timeleft;
        }

        CommonUtils.sleepMs(LOG, toSleep);
        timeleft = deadline - System.currentTimeMillis();
      }
    }
    return completed;
  }

  /**
   * Convenience method for {@code #persistAndWait(fs, uri, persistenceWaitTime, -1)}.
   * i.e. wait for an indefinite period of time to persist. This will block
   * for an indefinite period of time if the path is never persisted. Use with care.
   *
   * @param fs {@link FileSystem} to carry out Alluxio operations
   * @param uri the uri of the file to persist
   * @param persistenceWaitTime the persistence wait time
   */
  public static void persistAndWait(final FileSystem fs, final AlluxioURI uri,
      long persistenceWaitTime) throws FileDoesNotExistException, IOException, AlluxioException,
      TimeoutException, InterruptedException {
    persistAndWait(fs, uri, persistenceWaitTime, -1);
  }

  /**
   * Persists the given path to the under file system and returns once the persist is complete.
   * Note that if this method times out, the persist may still occur after the timeout period.
   *
   * @param fs {@link FileSystem} to carry out Alluxio operations
   * @param uri the uri of the file to persist
   * @param persistenceWaitTime the initial persistence wait time
   * @param timeoutMs max amount of time to wait for persist in milliseconds. -1 to wait
   *                  indefinitely
   * @throws TimeoutException if the persist takes longer than the timeout
   */
  public static void persistAndWait(final FileSystem fs, final AlluxioURI uri,
      long persistenceWaitTime, int timeoutMs) throws FileDoesNotExistException, IOException,
      AlluxioException, TimeoutException, InterruptedException {
    LOG.debug("Start to persist {}", uri.getPath());
    fs.persist(uri, ScheduleAsyncPersistencePOptions
        .newBuilder().setPersistenceWaitTime(persistenceWaitTime).build());
    LOG.debug("Waiting for persist result for path {}, with persistence wait time {}"
            + " and time out {}",
        uri.getPath(), persistenceWaitTime, timeoutMs);
    CommonUtils.waitForResult(String.format("%s to be persisted", uri) , () -> {
      try {
        LOG.debug("Polling file system master for the status of {}", uri);
        return fs.getStatus(uri);
      } catch (Exception e) {
        LOG.debug("Exception when attempting to getStatus while waiting for persistence {}",
            e.toString());
        Throwables.throwIfUnchecked(e);
        throw new RuntimeException(e);
      }
    }, (status) -> status.isPersisted(),
        WaitForOptions.defaults().setTimeoutMs(timeoutMs).setInterval(100));
  }

  /**
   * Waits until the specified file has the desired percentage in Alluxio.
   *
   * @param fs the file system
   * @param uri the uri to check the percentage for
   * @param expectedPercentage the desired percentage
   */
  public static void waitForAlluxioPercentage(final FileSystem fs, final AlluxioURI uri,
      int expectedPercentage) throws TimeoutException, InterruptedException {
    CommonUtils
        .waitFor(uri.toString() + " is " + expectedPercentage + "% stored in Alluxio", () -> {
          try {
            return fs.getStatus(uri).getInAlluxioPercentage() == expectedPercentage;
          } catch (Exception e) {
            // ignore
            return false;
          }
        }, WaitForOptions.defaults().setTimeoutMs(30 * Constants.SECOND_MS));
  }
}
