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

package alluxio.master.file;

import alluxio.master.file.meta.MutableInodeDirectory;
import alluxio.master.file.meta.MutableInodeFile;
import alluxio.master.journal.JournalEntryMerger;
import alluxio.proto.journal.Journal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A file system journal entry merger which merges inode creation and inode update journals
 * on the same inode object into one.
 * This class is not thread-safe and should not be shared across threads.
 */
@ThreadSafe
public class FileSystemJournalEntryMerger implements JournalEntryMerger {
  /** Persists merged journal entries. */
  private final List<Journal.JournalEntry> mJournalEntries = new ArrayList<>();
  /** A map whose key is the inode id and value is the journal index in the mJournalEntries list.*/
  private final Map<Long, Integer> mEntriesMap = new HashMap<>();

  /**
   * Adds a new journal entry and merges it on the fly.
   * @param entry the new journal entry to add
   */
  @Override
  public synchronized void add(Journal.JournalEntry entry) {
    if (entry.hasInodeFile() || entry.hasInodeDirectory()) {
      mJournalEntries.add(entry);
      mEntriesMap.put(getInodeId(entry), mJournalEntries.size() - 1);
    }
    else if (
        entry.hasUpdateInode() || entry.hasUpdateInodeFile() || entry.hasUpdateInodeDirectory()
    ) {
      if (!mEntriesMap.containsKey(getInodeId(entry))) {
        mJournalEntries.add(entry);
        return;
      }
      int index = mEntriesMap.get(getInodeId(entry));
      Journal.JournalEntry existingEntry = mJournalEntries.get(index);
      if (existingEntry.hasInodeFile()) {
        MutableInodeFile inodeFile =
            MutableInodeFile.fromJournalEntry(existingEntry.getInodeFile());
        if (entry.hasUpdateInode()) {
          inodeFile.updateFromEntry(entry.getUpdateInode());
        } else if (entry.hasUpdateInodeFile()) {
          inodeFile.updateFromEntry(entry.getUpdateInodeFile());
        }
        mJournalEntries.set(index, inodeFile.toJournalEntry());
      }
      if (existingEntry.hasInodeDirectory()) {
        MutableInodeDirectory inodeDirectory =
            MutableInodeDirectory.fromJournalEntry(existingEntry.getInodeDirectory());
        if (entry.hasUpdateInode()) {
          inodeDirectory.updateFromEntry(entry.getUpdateInode());
        } else if (entry.hasUpdateInodeDirectory()) {
          inodeDirectory.updateFromEntry(entry.getUpdateInodeDirectory());
        }
        mJournalEntries.set(index, inodeDirectory.toJournalEntry());
      }
    } else {
      mJournalEntries.add(entry);
    }
  }

  @Override
  public synchronized List<Journal.JournalEntry> getMergedJournalEntries() {
    return Collections.unmodifiableList(mJournalEntries);
  }

  @Override
  public synchronized void clear() {
    mEntriesMap.clear();
    mJournalEntries.clear();
  }

  private long getInodeId(Journal.JournalEntry entry) {
    if (entry.hasInodeDirectory()) {
      return entry.getInodeDirectory().getId();
    }
    if (entry.hasInodeFile()) {
      return entry.getInodeFile().getId();
    }
    if (entry.hasUpdateInode()) {
      return entry.getUpdateInode().getId();
    }
    if (entry.hasUpdateInodeFile()) {
      return entry.getUpdateInodeFile().getId();
    }
    if (entry.hasUpdateInodeDirectory()) {
      return entry.getUpdateInodeDirectory().getId();
    }
    throw new RuntimeException("Unsupported JournalEntry type. The following entries are supported:"
        + "InodeDirectory/InodeFile/UpdateInode/UpdateInodeFile/UpdateInodeDirectory");
  }
}
