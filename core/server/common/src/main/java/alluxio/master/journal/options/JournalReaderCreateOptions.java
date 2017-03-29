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
 * Options to create a journal reader.
 */
public class JournalReaderCreateOptions {
  /** The first log sequence number to read in the journal reader. */
  private long mNextSequenceNumber;
  /** Whether the journal reader is running in a primary master. */
  private boolean mPrimary;

  private JournalReaderCreateOptions() {} // prevent instantiation

  /**
   * @return the default journal reader create options
   */
  public static JournalReaderCreateOptions defaults() {
    return new JournalReaderCreateOptions();
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
  public boolean getPrimary() {
    return mPrimary;
  }

  /**
   * @param nextSequenceNumber the next sequence number
   * @return the updated options
   */
  public JournalReaderCreateOptions setNextSequenceNumber(long nextSequenceNumber) {
    mNextSequenceNumber = nextSequenceNumber;
    return this;
  }

  /**
   * @param primary whether jorunal reader is to run in a primary master
   * @return the updated options
   */
  public JournalReaderCreateOptions setPrimary(boolean primary) {
    mPrimary = primary;
    return this;
  }
}
