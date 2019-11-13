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

import alluxio.util.URIUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.UUID;

import javax.annotation.concurrent.ThreadSafe;
import javax.annotation.Nullable;

/**
 * A simple data structure that represents a journal file.
 * There are four types of files (SN is short for sequence number):
 * 1. Temporary file: the file name is a random UUID.
 * 2. Completed log file: [start SN]-[end SN] (the start SN is inclusive, the end SN is exclusive).
 * 3. Incomplete log file (a.k.a the current log): [start SN]-[LONG.MAX_VALUE] (the start SN is
 *    inclusive).
 * 4. Checkpoint file: [0]-[end SN] (the end SN is exclusive).
 *
 * This data structure implements {@link Comparable} such that journal files can be sorted by the
 * end SNs.
 *
 * Note that the natural ordering of the class may not necessarily imply equality of objects. The
 * {@link #compareTo(UfsJournalFile)} implementation should not be used to determine equality.
 */
@ThreadSafe
@VisibleForTesting
public final class UfsJournalFile implements Comparable<UfsJournalFile> {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournalFile.class);

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
  static UfsJournalFile createCheckpointFile(URI location, long end) {
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
  static UfsJournalFile createLogFile(URI location, long start, long end) {
    return new UfsJournalFile(location, start, end, false);
  }

  /**
   * Creates a temporary checkpoint file.
   *
   * @param location the file location
   * @return the file
   */
  static UfsJournalFile createTmpCheckpointFile(URI location) {
    return new UfsJournalFile(location, UfsJournal.UNKNOWN_SEQUENCE_NUMBER,
        UfsJournal.UNKNOWN_SEQUENCE_NUMBER, false);
  }

  /**
   * Encodes a checkpoint location under the checkpoint directory.
   *
   * @param journal the UFS journal instance
   * @param end the end sequence number (exclusive)
   * @return the location
   */
  static URI encodeCheckpointFileLocation(UfsJournal journal, long end) {
    String filename = String.format("0x%x-0x%x", 0, end);
    URI location = URIUtils.appendPathOrDie(journal.getCheckpointDir(), filename);
    return location;
  }

  /**
   * Encodes a log location under the log directory.
   *
   * @param journal the UFS journal instance
   * @param start the start sequence number (inclusive)
   * @param end the end sequence number (exclusive)
   * @return the location
   */
  static URI encodeLogFileLocation(UfsJournal journal, long start, long end) {
    String filename = String.format("0x%x-0x%x", start, end);
    URI location = URIUtils.appendPathOrDie(journal.getLogDir(), filename);
    return location;
  }

  /**
   * Encodes a temporary location under the temporary directory.
   *
   * @param journal the UFS journal instance*
   * @return the location
   */
  static URI encodeTemporaryCheckpointFileLocation(UfsJournal journal) {
    return URIUtils.appendPathOrDie(journal.getTmpDir(), UUID.randomUUID().toString());
  }

  /**
   * Decodes a checkpoint or a log file name into a {@link UfsJournalFile}.
   *
   * @param journal the UFS journal instance
   * @param filename the filename
   * @return the instance of {@link UfsJournalFile}, null if the file invalid
   */
  @Nullable
  static UfsJournalFile decodeLogFile(UfsJournal journal, String filename) {
    URI location = URIUtils.appendPathOrDie(journal.getLogDir(), filename);
    try {
      String[] parts = filename.split("-");

      // There can be temporary files in logs directory. Skip them.
      if (parts.length != 2) {
        return null;
      }
      long start = Long.decode(parts[0]);
      long end = Long.decode(parts[1]);
      return UfsJournalFile.createLogFile(location, start, end);
    } catch (IllegalStateException e) {
      LOG.error("Illegal journal file {}.", location);
      throw e;
    } catch (NumberFormatException e) {
      // There can be temporary files (e.g. created for rename).
      return null;
    }
  }

  /**
   * Decodes a checkpoint file name into a {@link UfsJournalFile}.
   *
   * @param journal the UFS journal instance
   * @param filename the filename
   * @return the instance of {@link UfsJournalFile}, null if the file invalid
   */
  @Nullable
  static UfsJournalFile decodeCheckpointFile(UfsJournal journal, String filename) {
    URI location = URIUtils.appendPathOrDie(journal.getCheckpointDir(), filename);
    try {
      String[] parts = filename.split("-");

      // There can be temporary files in logs directory. Skip them.
      if (parts.length != 2) {
        return null;
      }
      long start = Long.decode(parts[0]);
      long end = Long.decode(parts[1]);

      Preconditions.checkState(start == 0);
      return UfsJournalFile.createCheckpointFile(location, end);
    } catch (IllegalStateException e) {
      LOG.error("Illegal journal file {}.", location);
      throw e;
    } catch (NumberFormatException e) {
      // There can be temporary files (e.g. created for rename).
      return null;
    }
  }

  /**
   * Decodes a temporary checkpoint file name into a {@link UfsJournalFile}.
   *
   * @param journal the UFS journal instance
   * @param filename the temporary checkpoint file name
   * @return the instance of {@link UfsJournalFile}
   */
  static UfsJournalFile decodeTemporaryCheckpointFile(UfsJournal journal, String filename) {
    URI location = URIUtils.appendPathOrDie(journal.getTmpDir(), filename);
    return UfsJournalFile.createTmpCheckpointFile(location);
  }

  /**
   * @return the file location
   */
  @VisibleForTesting
  public URI getLocation() {
    return mLocation;
  }

  /**
   * @return the start sequence number (inclusive)
   */
  long getStart() {
    return mStart;
  }

  /**
   * @return the end sequence number (exclusive)
   */
  long getEnd() {
    return mEnd;
  }

  /**
   * @return whether it is a committed checkpoint file
   */
  boolean isCheckpoint() {
    return mIsCheckpoint;
  }

  /**
   * @return whether it is a completed log
   */
  boolean isCompletedLog() {
    return !isCheckpoint() && mEnd != UfsJournal.UNKNOWN_SEQUENCE_NUMBER;
  }

  /**
   * @return whether it is incomplete log (a.k.a. the current log)
   */
  boolean isIncompleteLog() {
    return mStart != UfsJournal.UNKNOWN_SEQUENCE_NUMBER
        && mEnd == UfsJournal.UNKNOWN_SEQUENCE_NUMBER;
  }

  /**
   * @return whether it is a temporary checkpoint
   */
  boolean isTmpCheckpoint() {
    return mStart == UfsJournal.UNKNOWN_SEQUENCE_NUMBER
        && mEnd == UfsJournal.UNKNOWN_SEQUENCE_NUMBER;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("location", mLocation).add("start", mStart)
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

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }

    if (!(o instanceof UfsJournalFile)) {
      return false;
    }

    UfsJournalFile other = (UfsJournalFile) o;

    return mLocation.equals(other.mLocation)
        && mIsCheckpoint == other.mIsCheckpoint
        && mStart == other.mStart
        && mEnd == other.mEnd;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mLocation, mIsCheckpoint, mStart, mEnd);
  }
}

