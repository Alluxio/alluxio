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

/**
 * Options to create a {@link alluxio.master.journal.JournalWriter}.
 */
public final class JournalWriterCreateOptions {
  /** The first journal log entry sequence number to write. */
  private long mNextSequenceNumber;
  /** Whether this is a primary master. */
  private boolean mPrimary;

  private JournalWriterCreateOptions() {} // prevent instantiation

  /**
   * @return the default options
   */
  public static JournalWriterCreateOptions defaults() {
    return new JournalWriterCreateOptions();
  }

  /**
   * @return the next sequence number
   */
  public long getNextSequenceNumber() {
    return mNextSequenceNumber;
  }

  /**
   * @return whether it is a primary master
   */
  public boolean getPrimary() {
    return mPrimary;
  }

  /**
   * @param nextSequenceNumber the next sequence number
   * @return the updated options
   */
  public JournalWriterCreateOptions setNextSequenceNumber(long nextSequenceNumber) {
    mNextSequenceNumber = nextSequenceNumber;
    return this;
  }

  /**
   * @param primary whether it is primary master
   * @return the updated options
   */
  public JournalWriterCreateOptions setPrimary(boolean primary) {
    mPrimary = primary;
    return this;
  }
}
