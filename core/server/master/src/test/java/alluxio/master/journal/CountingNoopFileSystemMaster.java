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

import alluxio.master.NoopMaster;
import alluxio.proto.journal.Journal;

import java.util.NoSuchElementException;

/**
 * A Noop FileSystemMaster which counts journal entries applied.
 * A delay can be specified to simulate the time it takes to process a journal entry.
 */
public class CountingNoopFileSystemMaster extends NoopMaster {
  public static final String ENTRY_DOES_NOT_EXIST = "The entry to delete does not exist!";

  /** Tracks how many entries have been applied to master. */
  private long mApplyCount = 0;
  /** Artificial delay to emulate processing time while applying entries, in ms. */
  private long mApplyDelay;

  public CountingNoopFileSystemMaster() {
    mApplyDelay = -1;
  }

  private CountingNoopFileSystemMaster(long timeMs) {
    mApplyDelay = timeMs;
  }
  
  public static CountingNoopFileSystemMaster withApplyDelay(long timeMs) {
    return new CountingNoopFileSystemMaster(timeMs);
  }

  @Override
  public boolean processJournalEntry(Journal.JournalEntry entry) {
    if (mApplyDelay != -1) {
      try {
        Thread.sleep(mApplyDelay);
      } catch (InterruptedException e) {
        // Do not interfere with interrupt handling.
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        // do nothing.
      }
    }
    mApplyCount++;
    // Throw error on a special entry
    if (entry.hasDeleteFile()) {
      throw new NoSuchElementException(ENTRY_DOES_NOT_EXIST);
    }
    return true;
  }

  /**
   * Sets an artificial delay for each apply call.
   *
   * @param timeMs delay in ms
   */
  public void setApplyDelay(long timeMs) {
    mApplyDelay = timeMs;
  }

  @Override
  public void resetState() {
    mApplyCount = 0;
  }

  /**
   * @return how many entries are applied
   */
  public long getApplyCount() {
    return mApplyCount;
  }

  @Override
  public String getName() {
    // RaftJournalWriter doesn't accept empty journal entries. FileSystemMaster is returned here
    // according to injected entry type during the test.
    return "FileSystemMaster";
  }
}
