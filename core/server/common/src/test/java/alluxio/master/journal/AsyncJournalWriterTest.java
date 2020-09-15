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

package alluxio.master.journal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.proto.journal.Journal.JournalEntry;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

/**
 * Unit tests for {@link AsyncJournalWriter}.
 */
public class AsyncJournalWriterTest {

  private JournalWriter mMockJournalWriter;
  private AsyncJournalWriter mAsyncJournalWriter;

  @After
  public void after() throws Exception {
    ServerConfiguration.reset();
  }

  private void setupAsyncJournalWriter(boolean batchingEnabled) throws Exception {
    if (batchingEnabled) {
      ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_FLUSH_BATCH_TIME_MS, "500ms");
    } else {
      ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_FLUSH_BATCH_TIME_MS, "0ms");
    }

    mMockJournalWriter = mock(JournalWriter.class);
    doNothing().when(mMockJournalWriter).write(any(JournalEntry.class));
    doNothing().when(mMockJournalWriter).flush();
    mAsyncJournalWriter = new AsyncJournalWriter(mMockJournalWriter, Collections::emptySet);
  }

  /**
   * Simulation for normal write and flush.
   *
   * @param batchingEnabled whether to use a batch flush or not
   */
  public void writesAndFlushesInternal(boolean batchingEnabled) throws Exception {
    setupAsyncJournalWriter(batchingEnabled);
    int entries = 5;

    for (int i = 0; i < entries; i++) {
      long flushCounter = mAsyncJournalWriter.appendEntry(JournalEntry.getDefaultInstance());
      // Assuming the flush counter starts from 0.
      assertEquals(i + 1, flushCounter);
    }

    for (int i = 1; i <= entries; i++) {
      mAsyncJournalWriter.flush(i);
    }
    verify(mMockJournalWriter, atLeastOnce()).flush();
  }

  @Test(timeout = 10000)
  public void writesAndFlushes() throws Exception {
    writesAndFlushesInternal(false);
  }

  @Test(timeout = 10000)
  public void writesAndFlushesWithBatching() throws Exception {
    writesAndFlushesInternal(true);
  }

  /**
   * Simulates that exception occurs when writing {@link JournalEntry}.
   *
   * @param batchingEnabled whether to use a batch flush or not
   */
  public void failedWriteInternal(boolean batchingEnabled) throws Exception {
    setupAsyncJournalWriter(batchingEnabled);

    // Start failing journal writes.
    // PS: Need to stop the writer before mocking because mMockJournalWriter could be
    // acceessed concurrently by the internal flush thread.
    mAsyncJournalWriter.stop();
    doThrow(new IOException("entry write failed")).when(mMockJournalWriter)
            .write(any(JournalEntry.class));
    mAsyncJournalWriter.start();

    int entries = 5;

    for (int i = 0; i < entries; i++) {
      long flushCounter = mAsyncJournalWriter.appendEntry(JournalEntry.getDefaultInstance());
      // Assuming the flush counter starts from 0
      assertEquals(i + 1, flushCounter);
    }

    // Flushes should fail.
    for (int i = 1; i <= entries; i++) {
      try {
        mAsyncJournalWriter.flush(1);
        fail("journal flush should not succeed if journal write fails.");
      } catch (IOException e) {
        // This is expected.
      }
    }

    // Allow journal writes to succeed.
    // PS: Need to stop the writer before mocking because mMockJournalWriter could be
    // acceessed concurrently by the internal flush thread.
    mAsyncJournalWriter.stop();
    doNothing().when(mMockJournalWriter).write(any(JournalEntry.class));
    mAsyncJournalWriter.start();

    // Flushes should succeed.
    for (int i = 1; i <= entries; i++) {
      mAsyncJournalWriter.flush(i);
    }
    verify(mMockJournalWriter, atLeastOnce()).flush();
  }

  @Test(timeout = 10000)
  public void failedWrite() throws Exception {
    failedWriteInternal(false);
  }

  @Test(timeout = 10000)
  public void failedWriteWithBatching() throws Exception {
    failedWriteInternal(true);
  }

  /**
   * Simulates that exception occurs when flush.
   *
   * @param batchingEnabled whether to use a batch flush or not
   */
  public void failedFlushInternal(boolean batchingEnabled) throws Exception {
    setupAsyncJournalWriter(batchingEnabled);
    int entries = 5;

    // Start failing journal flushes.
    // PS: Need to stop the writer before mocking because mMockJournalWriter could be
    // acceessed concurrently by the internal flush thread.
    mAsyncJournalWriter.stop();
    doThrow(new IOException("flush failed")).when(mMockJournalWriter).flush();
    mAsyncJournalWriter.start();

    for (int i = 0; i < entries; i++) {
      long flushCounter = mAsyncJournalWriter.appendEntry(JournalEntry.getDefaultInstance());
      // Assuming the flush counter starts from 0
      assertEquals(i + 1, flushCounter);
    }

    // Flushes should fail.
    for (int i = 1; i <= entries; i++) {
      try {
        mAsyncJournalWriter.flush(1);
        fail("journal flush should not succeed if journal flush fails.");
      } catch (IOException e) {
        // This is expected.
      }
    }

    // Allow journal flushes to succeed.
    // PS: Need to stop the writer before mocking because mMockJournalWriter could be
    // acceessed concurrently by the internal flush thread.
    mAsyncJournalWriter.stop();
    doNothing().when(mMockJournalWriter).flush();
    mAsyncJournalWriter.start();

    // Flushes should succeed.
    for (int i = 1; i <= entries; i++) {
      mAsyncJournalWriter.flush(i);
    }
    verify(mMockJournalWriter, atLeastOnce()).flush();
  }

  @Test(timeout = 10000)
  public void failedFlush() throws Exception {
    failedFlushInternal(false);
  }

  @Test(timeout = 10000)
  public void failedFlushWithBatching() throws Exception {
    failedFlushInternal(true);
  }
}
