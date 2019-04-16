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

package alluxio.master.journal.raft;

import alluxio.exception.JournalClosedException;
import alluxio.master.journal.JournalWriter;
import alluxio.proto.journal.Journal.JournalEntry;

import com.google.common.base.Preconditions;
import io.atomix.copycat.client.CopycatClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Class for writing entries to the Raft journal. Written entries are aggregated until flush is
 * called, then they are submitted as a single unit.
 */
@NotThreadSafe
public class RaftJournalWriter implements JournalWriter {
  private static final Logger LOG = LoggerFactory.getLogger(RaftJournalWriter.class);
  // How long to wait for a response from the cluster before giving up and trying again.
  private static final long PROCESS_TIMEOUT_S = 30;

  private final AtomicLong mNextSequenceNumberToWrite;
  private final AtomicLong mLastSubmittedSequenceNumber;
  private final AtomicLong mLastCommittedSequenceNumber;

  private final CopycatClient mClient;

  private volatile boolean mClosed;
  private JournalEntry.Builder mJournalEntryBuilder;

  /**
   * @param nextSequenceNumberToWrite the sequence number for the writer to begin writing at
   * @param client client for writing entries to the journal; the constructed journal writer owns
   *               this client and is responsible for closing it
   */
  public RaftJournalWriter(long nextSequenceNumberToWrite, CopycatClient client) {
    mNextSequenceNumberToWrite = new AtomicLong(nextSequenceNumberToWrite);
    mLastSubmittedSequenceNumber = new AtomicLong(-1);
    mLastCommittedSequenceNumber = new AtomicLong(-1);
    mClient = client;
    mClosed = false;
  }

  @Override
  public void write(JournalEntry entry) throws JournalClosedException {
    if (mClosed) {
      throw new JournalClosedException("Cannot write to journal. Journal writer has been closed");
    }
    Preconditions.checkState(entry.getAllFields().size() <= 1,
        "Raft journal entries should never set multiple fields, but found %s", entry);
    if (mJournalEntryBuilder == null) {
      mJournalEntryBuilder = JournalEntry.newBuilder();
    }
    mJournalEntryBuilder.addJournalEntries(entry.toBuilder()
        .setSequenceNumber(mNextSequenceNumberToWrite.getAndIncrement()).build());
  }

  @Override
  public void flush() throws IOException, JournalClosedException {
    if (mClosed) {
      throw new JournalClosedException("Cannot flush. Journal writer has been closed");
    }
    if (mJournalEntryBuilder != null) {
      long flushSN = mNextSequenceNumberToWrite.get() - 1;
      try {
        // It is ok to submit the same entries multiple times because we de-duplicate by sequence
        // number when applying them. This could happen if submit fails and we re-submit the same
        // entry on retry.
        mLastSubmittedSequenceNumber.set(flushSN);
        mClient.submit(new JournalEntryCommand(mJournalEntryBuilder.build())).get(PROCESS_TIMEOUT_S,
            TimeUnit.SECONDS);
        mLastCommittedSequenceNumber.set(flushSN);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      } catch (ExecutionException e) {
        throw new IOException(e.getCause());
      } catch (TimeoutException e) {
        throw new IOException(
            String.format("Timed out after waiting %s seconds for journal entries to be processed",
                PROCESS_TIMEOUT_S), e);
      }
      mJournalEntryBuilder = null;
    }
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
    LOG.info("Closing journal writer. Last sequence numbers written/submitted/committed: {}/{}/{}",
        mNextSequenceNumberToWrite.get() - 1, mLastSubmittedSequenceNumber.get(),
        mLastCommittedSequenceNumber.get());
    closeClient();
  }

  /**
   * @return the next sequence number that will be written by this writer
   */
  public long getNextSequenceNumberToWrite() {
    return mNextSequenceNumberToWrite.get();
  }

  private void closeClient() {
    try {
      mClient.close().get(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      LOG.debug("Failed to close raft client: {}", e.toString());
    } catch (TimeoutException e) {
      LOG.debug("Failed to close raft client after 10 seconds");
    }
  }
}
