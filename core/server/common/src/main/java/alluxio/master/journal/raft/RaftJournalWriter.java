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

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.JournalClosedException;
import alluxio.master.journal.JournalWriter;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.FormatUtils;
import com.google.common.base.Preconditions;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Class for writing entries to the Raft journal. Written entries are aggregated until flush is
 * called, then they are submitted as a single unit.
 */
@NotThreadSafe
public class RaftJournalWriter implements JournalWriter {
  private static final Logger LOG = LoggerFactory.getLogger(RaftJournalWriter.class);
  // How long to wait for a response from the cluster before giving up and trying again.
  private final long mWriteTimeoutMs;
  private final long mEntrySizeMax;
  private final long mFlushBatchBytes;

  private final AtomicLong mNextSequenceNumberToWrite;
  private final AtomicLong mLastSubmittedSequenceNumber;
  private final AtomicLong mLastCommittedSequenceNumber;

  private final LocalFirstRaftClient mClient;

  private volatile boolean mClosed;
  private JournalEntry.Builder mJournalEntryBuilder;
  private final AtomicLong mCurrentJournalEntrySize;

  /**
   * @param nextSequenceNumberToWrite the sequence number for the writer to begin writing at
   * @param client client for writing entries to the journal; the constructed journal writer owns
   *               this client and is responsible for closing it
   */
  public RaftJournalWriter(long nextSequenceNumberToWrite,
      LocalFirstRaftClient client) {
    LOG.info("Journal writer created starting at SN#{}", nextSequenceNumberToWrite);
    mNextSequenceNumberToWrite = new AtomicLong(nextSequenceNumberToWrite);
    mLastSubmittedSequenceNumber = new AtomicLong(-1);
    mLastCommittedSequenceNumber = new AtomicLong(-1);
    mCurrentJournalEntrySize = new AtomicLong(0);
    mClient = client;
    mClosed = false;
    mWriteTimeoutMs =
        ServerConfiguration.getMs(PropertyKey.MASTER_EMBEDDED_JOURNAL_WRITE_TIMEOUT);
    // journal entry size max is the hard limit set by underlying ratis
    // use a smaller value to guarantee we don't pass the hard limit
    mEntrySizeMax = ServerConfiguration
        .getBytes(PropertyKey.MASTER_EMBEDDED_JOURNAL_ENTRY_SIZE_MAX);
    mFlushBatchBytes = mEntrySizeMax / 3;
  }
  
  private String formatEntry(JournalEntry entry) {
    Queue<JournalEntry> entries = new LinkedList<>(entry.getJournalEntriesList());
    List<JournalEntry> children = new ArrayList<>();
    while (!entries.isEmpty()) {
      JournalEntry poll = entries.poll();
      entries.addAll(poll.getJournalEntriesList());
      children.add(poll);
    }
    return String.format("entry #%s (children entry #s: %s)", entry.getSequenceNumber(),
            children.stream().map(JournalEntry::getSequenceNumber)
                    .sorted(Comparator.reverseOrder()).collect(Collectors.toList()));
  }

  @Override
  public void write(JournalEntry entry) throws IOException, JournalClosedException {
    LOG.info("writing {}", formatEntry(entry));
    if (mClosed) {
      String s = String.format("Cannot write entry #%s to journal. Journal writer has been closed",
              entry.getSequenceNumber());
      LOG.info(s);
      throw new JournalClosedException(s);
    }
    Preconditions.checkState(entry.getAllFields().size() <= 1,
        "Raft journal entries should never set multiple fields, but found %s", entry);
    long l1 = mCurrentJournalEntrySize.get();
    if (l1 > mFlushBatchBytes) {
      LOG.info("currentJournalEntrySize={}, flushBatchBytes={}", l1, mFlushBatchBytes);
      flush();
    }
    if (mJournalEntryBuilder == null) {
      mJournalEntryBuilder = JournalEntry.newBuilder();
      mCurrentJournalEntrySize.set(0);
    }
    LOG.info("Writing entry #{}: {}", mNextSequenceNumberToWrite, entry);
    long SN = mNextSequenceNumberToWrite.getAndIncrement();
    JournalEntry build = entry.toBuilder().setSequenceNumber(SN).build();
    LOG.info("JournalEntry.Builder receives {}", formatEntry(build));
    mJournalEntryBuilder.addJournalEntries(build);
    long size = entry.getSerializedSize();
    if (size > mEntrySizeMax) {
      LOG.error("Journal entry size ({}) is bigger than the max allowed size ({}) defined by {}",
          FormatUtils.getSizeFromBytes(size), FormatUtils.getSizeFromBytes(mEntrySizeMax),
          PropertyKey.MASTER_EMBEDDED_JOURNAL_ENTRY_SIZE_MAX.getName());
    }
    long l = mCurrentJournalEntrySize.addAndGet(size);
    LOG.info("currentJournalEntrySize incremented by {} to {}", size, l);
  }

  @Override
  public void flush() throws IOException, JournalClosedException {
    LOG.info("flushing");
    if (mClosed) {
      String s = "Cannot flush. Journal writer has been closed";
      LOG.info(s);
      throw new JournalClosedException(s);
    }
    if (mJournalEntryBuilder != null) {
      long flushSN = mNextSequenceNumberToWrite.get() - 1;
      LOG.info("flushSN={}", flushSN);
      try {
        // It is ok to submit the same entries multiple times because we de-duplicate by sequence
        // number when applying them. This could happen if submit fails and we re-submit the same
        // entry on retry.
        JournalEntry entry = mJournalEntryBuilder.build();
        Message message = RaftJournalSystem.toRaftMessage(entry);
        mLastSubmittedSequenceNumber.set(flushSN);
        LOG.info("last submitted sequence number={}", mLastSubmittedSequenceNumber);

        LOG.info("Flushing {} ({})", formatEntry(entry), message);

        RaftClientReply reply = mClient
            .sendAsync(message, TimeDuration.valueOf(mWriteTimeoutMs, TimeUnit.MILLISECONDS))
            .get(mWriteTimeoutMs, TimeUnit.MILLISECONDS);

        mLastCommittedSequenceNumber.set(flushSN);

        LOG.info("last committed sequence number={}", mLastCommittedSequenceNumber);
        if (reply.getException() != null) {
          LOG.info("Received exception from reply: {}", reply.getException().toString());
          throw reply.getException();
        }
      } catch (InterruptedException e) {
        LOG.info("Received exception: {}", e.toString());
        Thread.currentThread().interrupt();
        throw new IOException(e);
      } catch (ExecutionException e) {
        LOG.info("Received exception: {}", e.toString());
        throw new IOException(e.getCause());
      } catch (TimeoutException e) {
        LOG.info("Received exception: {}", e.toString());
        throw new IOException(String.format(
            "Timed out after waiting %s milliseconds for journal entries to be processed",
            mWriteTimeoutMs), e);
      }
      LOG.info("resetting journal entry builder");
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
      mClient.close();
    } catch (IOException e) {
      LOG.warn("Failed to close raft client: {}", e.toString());
    }
  }
}
