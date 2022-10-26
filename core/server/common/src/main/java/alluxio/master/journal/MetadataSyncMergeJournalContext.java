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

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A {@link FileSystemMergeJournalContext} used in MetadataSync (InodeSyncStream).
 *
 * This journal context never enforces a synchronous journal flush but only appends the journals
 * into the async journal writer. Metadata sync creates a bunch of inodes and with
 * {@link alluxio.conf.PropertyKey#MASTER_FILE_SYSTEM_MERGE_INODE_JOURNALS} enabled,
 * journals are committed as long as an inode path lock is released, which creates too many
 * journal persistence/ raft RPCs and may impair the metadata sync performance.
 *
 * Hence, this class only flushes journals async on flush() and close() and the underlying
 * journals in the async journal writer will be committed when the underlying JournalContext
 * gets closed.
 *
 * Each metadata sync job should have its own
 * {@link MetadataSyncMergeJournalContext} instance.
 */
@ThreadSafe
public class MetadataSyncMergeJournalContext extends FileSystemMergeJournalContext {
  /**
   * Constructs a {@link MetadataSyncMergeJournalContext}.
   *
   * @param journalContext the journal context to wrap
   * @param journalEntryMerger the merger which merges multiple journal entries into one
   */
  public MetadataSyncMergeJournalContext(
      JournalContext journalContext, JournalEntryMerger journalEntryMerger) {
    super(journalContext, journalEntryMerger);
  }

  @Override
  public synchronized void flush() {
    appendMergedJournals();
  }

  @Override
  public synchronized void close() {
    appendMergedJournals();
    // underlying JournalContext won't be closed here because it's still used by
    // the rpc thread.
  }

  /**
   * @return the journal merger, used in unit test
   */
  @VisibleForTesting
  public synchronized JournalEntryMerger getMerger() {
    return mJournalEntryMerger;
  }
}
