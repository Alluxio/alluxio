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

import alluxio.AlluxioURI;
import alluxio.exception.status.UnavailableException;
import alluxio.proto.journal.Journal.JournalEntry;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Context for merging journal entries together for a wrapped journal context.
 *
 * This is used so that we can combine several journal entries into one using a merge
 * function. This prevents partial writes of these journal entries causing system to
 * be left in an inconsistent state. For example, createFile without completing the file.
 *
 * This class will only merge the relevant inode entries for a particular URL.
 * File operations to other URIs will be passed to the underlying JournalContext,
 * and will not be buffered.
 *
 * Note that any buffered journal entries are not persisted and they will only be persisted
 * when close is called on them. Closing the MergeJournalContext will also not close
 * the enclosed journal context.
 */
@NotThreadSafe
public final class InodeSyncMergeJournalContext implements JournalContext {
  // It will log a warning if the number of buffered journal entries exceed 100
  public static final int MAX_ENTRIES = 100;
  private static final long INVALID_FILE_ID = -1;

  private static final Logger LOG = LoggerFactory.getLogger(InodeSyncMergeJournalContext.class);

  private final JournalContext mJournalContext;
  private final AlluxioURI mUri;
  private final UnaryOperator<List<JournalEntry>> mMergeOperator;
  private final List<JournalEntry> mJournalEntries = new ArrayList<>();
  private long mFileId = INVALID_FILE_ID;

  /**
   * Constructs a {@link InodeSyncMergeJournalContext}.
   * @param journalContext the journal context to wrap
   * @param uri Alluxio URI that needs merging
   * @param merger merging function which will merge multiple journal entries into one
   */
  public InodeSyncMergeJournalContext(JournalContext journalContext,
                                      AlluxioURI uri, UnaryOperator<List<JournalEntry>> merger) {
    Preconditions.checkNotNull(journalContext, "journalContext");
    mJournalContext = journalContext;
    mMergeOperator = merger;
    mUri = uri;
  }

  @Override
  public void append(JournalEntry entry) {
    boolean merge = false;
    // We are merging these statements because they have the potential to leave incomplete files.
    // We can add additional statement to be merged if necessary.
    if (entry.hasInodeFile() || entry.hasUpdateInode() || entry.hasUpdateInodeFile()) {
      if (entry.hasInodeFile() && entry.getInodeFile().getPath().equals(mUri.getPath())) {
        merge = true;
        mFileId = entry.getInodeFile().getId();
      } else if (entry.hasUpdateInodeFile() && mFileId != INVALID_FILE_ID
          && entry.getUpdateInodeFile().getId() == mFileId) {
        merge = true;
      } else if (entry.hasUpdateInode() && mFileId != INVALID_FILE_ID
          && entry.getUpdateInode().getId() == mFileId) {
        merge = true;
      }
    }
    if (merge) {
      // to be merged
      mJournalEntries.add(entry);
    } else {
      // pass through
      mJournalContext.append(entry);
    }
  }

  @Override
  public void close() throws UnavailableException {
    if (mJournalEntries.size() > MAX_ENTRIES) {
      LOG.debug("MergeJournalContext has " + mJournalEntries.size()
          + " entries, over the limit of " + MAX_ENTRIES);
    }
    List<JournalEntry> mergedEntries = mMergeOperator.apply(mJournalEntries);
    mergedEntries.forEach(mJournalContext::append);
    // Note that we do not close the enclosing journal context here
  }
}
