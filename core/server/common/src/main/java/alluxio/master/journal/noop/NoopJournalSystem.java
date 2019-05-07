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

package alluxio.master.journal.noop;

import alluxio.master.Master;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalSystem;

/**
 * Journal system which doesn't do anything.
 */
public final class NoopJournalSystem implements JournalSystem {
  /**
   * Constructs a new {@link NoopJournalSystem}.
   */
  public NoopJournalSystem() {}

  @Override
  public Journal createJournal(Master master) {
    return new NoopJournal();
  }

  @Override
  public void gainPrimacy() {
    return;
  }

  @Override
  public void losePrimacy() {
    return;
  }

  @Override
  public boolean isFormatted() {
    return true;
  }

  @Override
  public boolean isEmpty() {
    return true;
  }

  @Override
  public void format() {}

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public void checkpoint() {}
}
