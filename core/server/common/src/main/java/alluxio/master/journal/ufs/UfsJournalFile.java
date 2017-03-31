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

package alluxio.master.journal.ufs;

import com.google.common.base.Objects;

import java.net.URI;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A simple data structure that represents a journal file.
 * There are four types of files (SN is short for sequence number):
 * 1. Temporary file: the file name is a random UUID.
 * 2. Completed log file: [start SN]-[end SN] (the start SN is inclusive, the end SN is exclusive).
 * 3. Incomplete log file (a.k.a the current log): [start SN]-[LONG.MAX_VALUE] (the start SN is
 *    inclusive).
 * 4. Checkpoint file: [0]-[end SN] (the end SN is exclusive).
 *
 * This data structure implements {@link Comparable} such they can be sorted by the end SNs.
 */
@ThreadSafe
final public class UfsJournalFile implements Comparable<UfsJournalFile> {
  /** The location of the file. */
  private final URI mLocation;
  /** The start journal log entry sequence number (inclusive). */
  private final long mStart;
  /** The end journal log entry sequence number (exclusive). */
  private final long mEnd;
  /**
   * Whether this is a checkpoint file (the temporary checkpoint file is not considered as a
   * checkpoint file here).
   */
  private final boolean mIsCheckpoint;

  /**
   * Creates a journal file.
   *
   * @param location the file location
   * @param start the start sequence number (inclusive)
   * @param end the end sequence number (exclusive)
   * @param isCheckpoint whether this is a committed checkpoint file
   */
  private UfsJournalFile(URI location, long start, long end, boolean isCheckpoint) {
    mLocation = location;
    mStart = start;
    mEnd = end;
    mIsCheckpoint = isCheckpoint;
  }

  /**
   * Creates a committed checkpoint file.
   *
   * @param location the file location
   * @param end the end sequence number (exclusive)
   * @return the file
   */
  public static UfsJournalFile createCheckpointFile(URI location, long end) {
    return new UfsJournalFile(location, 0, end, true);
  }

  /**
   * Creates a journal log file.
   *
   * @param location the file location
   * @param start the start sequence number (inclusive)
   * @param end the end sequence number (exclusive)
   * @return the file
   */
  public static UfsJournalFile createLogFile(URI location, long start, long end) {
    return new UfsJournalFile(location, start, end, false);
  }

  /**
   * Creates a temporary checkpoint file.
   *
   * @param location the file location
   * @return the file
   */
  public static UfsJournalFile createTmpCheckpointFile(URI location) {
    return new UfsJournalFile(location, UfsJournal.UNKNOWN_SEQUENCE_NUMBER,
        UfsJournal.UNKNOWN_SEQUENCE_NUMBER, false);
  }

  /**
   * @return the file location
   */
  public URI getLocation() {
    return mLocation;
  }

  /**
   * @return the start sequence number (inclusive)
   */
  public long getStart() {
    return mStart;
  }

  /**
   * @return the end sequence number (exclusive)
   */
  public long getEnd() {
    return mEnd;
  }

  /**
   * @return whether it is a committed checkpoint file
   */
  public boolean isCheckpoint() {
    return mIsCheckpoint;
  }

  /**
   * @return whether it is a completed log
   */
  public boolean isCompletedLog() {
    return !isCheckpoint() && mEnd != UfsJournal.UNKNOWN_SEQUENCE_NUMBER;
  }

  /**
   * @return whether it is incomplete log (a.k.a. the current log)
   */
  public boolean isIncompleteLog() {
    return mStart != UfsJournal.UNKNOWN_SEQUENCE_NUMBER
        && mEnd == UfsJournal.UNKNOWN_SEQUENCE_NUMBER;
  }

  /**
   * @return whether it is a temporary checkpoint
   */
  public boolean isTmpCheckpoint() {
    return mStart == UfsJournal.UNKNOWN_SEQUENCE_NUMBER
        && mEnd == UfsJournal.UNKNOWN_SEQUENCE_NUMBER;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("location", mLocation).add("start", mStart)
        .add("end", mEnd).add("isCheckpoint", mIsCheckpoint).toString();
  }

  @Override
  public int compareTo(UfsJournalFile other) {
    long diff = mEnd - other.mEnd;
    if (diff < 0) {
      return -1;
    } else if (diff == 0) {
      return 0;
    } else {
      return 1;
    }
  }
}

