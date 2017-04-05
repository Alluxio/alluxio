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

package alluxio.master.journal.options;

import com.google.common.base.Objects;

import java.net.URI;

/**
 * Options to create a journal reader.
 */
public final class JournalReaderOptions {
  /** The first log sequence number to read in the journal reader. */
  private long mNextSequenceNumber;
  /** Whether the journal reader is running in a primary master. */
  private boolean mPrimary;

  /**
   * If set, read only from this file instead of the default journal location. This is used to write
   * tool to parse any file comprised of journal entries.
   */
  private URI mLocation;

  private JournalReaderOptions() {} // prevent instantiation

  /**
   * @return the default journal reader create options
   */
  public static JournalReaderOptions defaults() {
    return new JournalReaderOptions();
  }

  /**
   * @return the next sequence number
   */
  public long getNextSequenceNumber() {
    return mNextSequenceNumber;
  }

  /**
   * @return whether journal reader is to run in a primary master
   */
  public boolean isPrimary() {
    return mPrimary;
  }

  /**
   * @return the journal file location, null if not set
   */
  public URI getLocation() {
    return mLocation;
  }

  /**
   * @param nextSequenceNumber the next sequence number
   * @return the updated options
   */
  public JournalReaderOptions setNextSequenceNumber(long nextSequenceNumber) {
    mNextSequenceNumber = nextSequenceNumber;
    return this;
  }

  /**
   * @param primary whether jorunal reader is to run in a primary master
   * @return the updated options
   */
  public JournalReaderOptions setPrimary(boolean primary) {
    mPrimary = primary;
    return this;
  }

  /**
   * @param location the journal file location
   * @return the updated options
   */
  public JournalReaderOptions setLocation(URI location) {
    mLocation = location;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof JournalReaderOptions)) {
      return false;
    }
    JournalReaderOptions that = (JournalReaderOptions) o;
    return Objects.equal(mNextSequenceNumber, that.mNextSequenceNumber)
        && Objects.equal(mPrimary, that.mPrimary)
        && Objects.equal(mLocation, that.mLocation);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mNextSequenceNumber, mPrimary, mLocation);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("nextSequenceNumber", mNextSequenceNumber)
        .add("primary", mPrimary)
        .add("location", mLocation).toString();
  }
}
