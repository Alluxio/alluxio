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

@ThreadSafe
public class UfsJournalFile implements Comparable<UfsJournalFile> {
  /** The location of the file. */
  private final URI mLocation;
  private final long mStart;
  private final long mEnd;
  private final boolean mIsCheckpoint;

  private UfsJournalFile(URI location, long start, long end, boolean isCheckpoint) {
    mLocation = location;
    mStart = start;
    mEnd = end;
    mIsCheckpoint = isCheckpoint;
  }

  public static UfsJournalFile createCheckpointFile(URI location, long end) {
    return new UfsJournalFile(location, 0, end, true);
  }

  public static UfsJournalFile createLog(URI location, long start, long end) {
    return new UfsJournalFile(location, start, end, false);
  }

  public static UfsJournalFile createTmpCheckpoint(URI location) {
    return new UfsJournalFile(location, UfsJournal.UNKNOWN_SEQUENCE_NUMBER,
        UfsJournal.UNKNOWN_SEQUENCE_NUMBER, false);
  }

  public URI getLocation() {
    return mLocation;
  }

  public long getStart() {
    return mStart;
  }

  public long getEnd() {
    return mEnd;
  }

  public boolean isCheckpoint() {
    return mIsCheckpoint;
  }

  public boolean isCompletedLog() {
    return !isCheckpoint() && mEnd != UfsJournal.UNKNOWN_SEQUENCE_NUMBER;
  }

  public boolean isIncompleteLog() {
    return mStart != UfsJournal.UNKNOWN_SEQUENCE_NUMBER
        && mEnd == UfsJournal.UNKNOWN_SEQUENCE_NUMBER;
  }

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
    long diff = mEnd== other.mEnd? mStart - other.mStart : mEnd - other.mEnd;
    if (diff < 0) {
      return -1;
    } else if (diff == 0) {
      return 0;
    } else {
      return 1;
    }
  }
}
