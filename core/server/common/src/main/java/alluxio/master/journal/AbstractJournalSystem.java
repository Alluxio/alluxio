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

import alluxio.master.Master;
import alluxio.master.journal.sink.JournalSink;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Base implementation for journal systems.
 */
@ThreadSafe
public abstract class AbstractJournalSystem implements JournalSystem {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractJournalSystem.class);

  private boolean mRunning = false;

  protected final Map<String, Set<JournalSink>> mJournalSinks = new ConcurrentHashMap<>();

  @Override
  public synchronized void start() throws InterruptedException, IOException {
    Preconditions.checkState(!mRunning, "Journal is already running");
    startInternal();
    mRunning = true;
  }

  @Override
  public synchronized void stop() throws InterruptedException, IOException {
    Preconditions.checkState(mRunning, "Journal is not running");
    mRunning = false;
    stopInternal();
  }

  @Override
  public void addJournalSink(Master master, JournalSink journalSink) {
    mJournalSinks.compute(master.getName(), (key, entry) -> {
      if (entry == null) {
        Set<JournalSink> s = new HashSet<>();
        s.add(journalSink);
        return s;
      } else {
        entry.add(journalSink);
        return entry;
      }
    });
  }

  @Override
  public void removeJournalSink(Master master, JournalSink journalSink) {
    mJournalSinks.computeIfPresent(master.getName(), (key, entry) -> {
      if (entry != null) {
        entry.remove(journalSink);
        return entry;
      }
      return null;
    });
  }

  /**
   * Starts the journal system.
   */
  protected abstract void startInternal() throws InterruptedException, IOException;

  /**
   * Stops the journal system.
   */
  protected abstract void stopInternal() throws InterruptedException, IOException;
}
