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

import alluxio.collections.ConcurrentHashSet;
import alluxio.master.Master;
import alluxio.master.journal.sink.JournalSink;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.LockResource;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Base implementation for journal systems.
 */
@ThreadSafe
public abstract class AbstractJournalSystem implements JournalSystem {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractJournalSystem.class);

  private boolean mRunning = false;

  private final ReentrantReadWriteLock mSinkLock = new ReentrantReadWriteLock();
  private final Map<String, Set<JournalSink>> mJournalSinks = new ConcurrentHashMap<>();
  private final Set<JournalSink> mAllJournalSinks = new ConcurrentHashSet<>();

  @Override
  public synchronized void start() throws InterruptedException, IOException {
    Preconditions.checkState(!mRunning, "Journal is already running");
    startInternal();
    mRunning = true;
  }

  @Override
  public synchronized void stop() throws InterruptedException, IOException {
    Preconditions.checkState(mRunning, "Journal is not running");
    mAllJournalSinks.forEach(JournalSink::beforeShutdown);
    mRunning = false;
    stopInternal();
  }

  @Override
  public void addJournalSink(Master master, JournalSink journalSink) {
    try (LockResource r = new LockResource(mSinkLock.writeLock())) {
      mJournalSinks.computeIfAbsent(master.getName(), (key) -> new HashSet<>()).add(journalSink);
      mAllJournalSinks.add(journalSink);
    }
  }

  @Override
  public void removeJournalSink(Master master, JournalSink journalSink) {
    try (LockResource r = new LockResource(mSinkLock.writeLock())) {
      Set<JournalSink> sinks = mJournalSinks.get(master.getName());
      if (sinks != null) {
        sinks.remove(journalSink);
        if (sinks.isEmpty()) {
          mJournalSinks.remove(master.getName());
        }
      }
      // Compute the full set of sinks on removal. Simply removing it may be incorrect, if the same
      // sink is associated with other masters.
      mAllJournalSinks.clear();
      for (Set<JournalSink> s : mJournalSinks.values()) {
        mAllJournalSinks.addAll(s);
      }
    }
  }

  @Override
  public Set<JournalSink> getJournalSinks(@Nullable Master master) {
    try (LockResource r = new LockResource(mSinkLock.readLock())) {
      if (master == null) {
        return mAllJournalSinks;
      }
      return mJournalSinks.getOrDefault(master.getName(), Collections.emptySet());
    }
  }

  /**
   * Starts the journal system.
   */
  protected abstract void startInternal() throws InterruptedException, IOException;

  /**
   * Stops the journal system.
   */
  protected abstract void stopInternal() throws InterruptedException, IOException;

  protected void registerMetrics() {
    Map<String, Long> sequenceNumber = getCurrentSequenceNumbers();
    for (String masterName : sequenceNumber.keySet()) {
      MetricsSystem.registerGaugeIfAbsent(
          MetricKey.MASTER_JOURNAL_SEQUENCE_NUMBER.getName() + "." + masterName,
          () -> getCurrentSequenceNumbers().get(masterName));
    }
  }
}
