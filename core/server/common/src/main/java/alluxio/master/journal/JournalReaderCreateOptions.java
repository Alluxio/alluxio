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

public class JournalReaderCreateOptions {
  private long mNextSequenceNumber;
  private boolean mPrimary;

  private JournalReaderCreateOptions() {} // prevent instantiation

  public static JournalReaderCreateOptions defaults() {
    return new JournalReaderCreateOptions();
  }

  public long getNextSequenceNumber() {
    return mNextSequenceNumber;
  }

  public boolean getPrimary() {
    return mPrimary;
  }

  public JournalReaderCreateOptions setNextSequenceNumber(long nextSequenceNumber) {
    mNextSequenceNumber = nextSequenceNumber;
    return this;
  }

  public JournalReaderCreateOptions setPrimary(boolean primary) {
    mPrimary = primary;
    return this;
  }
}
