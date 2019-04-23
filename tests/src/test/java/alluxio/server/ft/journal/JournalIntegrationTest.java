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

package alluxio.server.ft.journal;

import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.status.CancelledException;
import alluxio.exception.status.UnavailableException;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalType;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Integration tests for implementations of {@link JournalSystem}.
 */
public class JournalIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(JournalIntegrationTest.class);

  @Rule
  public LocalAlluxioClusterResource mClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS.toString())
          .setProperty(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS, 0)
          .setProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.MUST_CACHE)
          .setNumWorkers(0)
          .build();

  private LocalAlluxioCluster mCluster;
  private AtomicReference<Exception> mException = new AtomicReference<>();

  @Before
  public void before() {
    mCluster = mClusterResource.get();
  }

  @Test
  public void recoverAfterSingleRestart() throws Exception {
    AlluxioURI testFile = new AlluxioURI("/testFile");
    mCluster.getClient().createFile(testFile).close();
    mCluster.restartMasters();
    assertTrue(mCluster.getClient().exists(testFile));
  }

  @Test
  public void recoverAfterDoubleRestart() throws Exception {
    AlluxioURI testFile = new AlluxioURI("/testFile");
    mCluster.getClient().createFile(testFile).close();
    mCluster.restartMasters();
    mCluster.restartMasters();
    assertTrue(mCluster.getClient().exists(testFile));
  }

  @Test
  public void concurrentWriteAndRestart() throws Exception {
    final FileToucher toucher = new FileToucher();
    Thread t = new Thread(toucher);
    t.start();
    try {
      CommonUtils.waitFor("20 files to be created", () -> toucher.getFilesTouched() >= 20,
          WaitForOptions.defaults().setTimeoutMs(5 * Constants.SECOND_MS));
      mCluster.restartMasters();
    } catch (Exception e) {
      // If an exception occurred, throw it instead of the timeout exception
      checkException();
      throw e;
    } finally {
      t.interrupt();
    }
    t.join();
    checkException();
    int numFiles = mCluster.getClient().listStatus(new AlluxioURI("/")).size();
    // There may be one extra file if the client was interrupted during the last RPC.
    assertTrue(numFiles == toucher.getFilesTouched() || numFiles == toucher.getFilesTouched() + 1);
  }

  /**
   * Throws any exception raised in another thread in this class.
   */
  private void checkException() throws Exception {
    if (mException.get() != null) {
      throw mException.get();
    }
  }

  private class FileToucher implements Runnable {
    private AtomicInteger mFilesTouched = new AtomicInteger(0);

    public void run() {
      FileSystem fs;
      try {
        fs = mCluster.getClient();
      } catch (IOException e) {
        mException.set(e);
        return;
      }
      while (!Thread.interrupted()) {
        try {
          fs.createFile(new AlluxioURI("/file-" + UUID.randomUUID()));
          mFilesTouched.incrementAndGet();
        } catch (FileAlreadyExistsException e) {
          // Ignore - we could have restarted after journaling the file write.
          mFilesTouched.incrementAndGet();
        } catch (UnavailableException e) {
          // Ignore - this happen if the client is interrupted while trying to connect.
        } catch (IOException | AlluxioException e) {
          if (e.getCause() instanceof CancelledException) {
            // Ignore - this happen if the client is interrupted while trying to connect.
          } else {
            mException.set(e);
          }
        }
      }
    }

    public int getFilesTouched() {
      return mFilesTouched.get();
    }
  }
}
