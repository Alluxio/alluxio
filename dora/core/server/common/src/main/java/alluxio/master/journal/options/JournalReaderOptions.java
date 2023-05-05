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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * Options to create a journal reader.
 */
public final class JournalReaderOptions {
  /** The first log sequence number to read in the journal reader. */
  private long mNextSequenceNumber;
  /** Whether the journal reader is running in a primary master. */
  private boolean mPrimary;

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
   * @param nextSequenceNumber the next sequence number
   * @return the updated options
   */
  public JournalReaderOptions setNextSequenceNumber(long nextSequenceNumber) {
    mNextSequenceNumber = nextSequenceNumber;
    return this;
  }

  /**
   * @param primary whether journal reader is to run in a primary master
   * @return the updated options
   */
  public JournalReaderOptions setPrimary(boolean primary) {
    mPrimary = primary;
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
        && Objects.equal(mPrimary, that.mPrimary);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mNextSequenceNumber, mPrimary);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("nextSequenceNumber", mNextSequenceNumber)
        .add("primary", mPrimary).toString();
  }
}
