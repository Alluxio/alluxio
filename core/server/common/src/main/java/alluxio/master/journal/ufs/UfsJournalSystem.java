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

import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.Master;
import alluxio.master.journal.AbstractJournalSystem;
import alluxio.master.journal.CatchupFuture;
import alluxio.master.journal.sink.JournalSink;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.retry.ExponentialTimeBoundedRetry;
import alluxio.retry.RetryPolicy;
import alluxio.util.CommonUtils;
import alluxio.util.URIUtils;
import alluxio.util.WaitForOptions;

import com.codahale.metrics.Timer;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Factory for under file storage journals.
 */
@NotThreadSafe
public class UfsJournalSystem extends AbstractJournalSystem {
  private static final Logger LOG = LoggerFactory.getLogger(UfsJournalSystem.class);

  private final URI mBase;
  private final long mQuietTimeMs;
  private ConcurrentHashMap<String, UfsJournal> mJournals;
  private long mInitialCatchupTimeMs = -1;

  /**
   * Creates a UFS journal system with the specified base location. When journals are created, their
   * names are appended to the base location. The created journals all function independently.
   *
   * @param base the base location for journals created by this factory
   * @param quietTimeMs before upgrading from SECONDARY to PRIMARY mode, the journal will wait until
   *        this duration has passed without any journal entries being written.
   */
  public UfsJournalSystem(URI base, long quietTimeMs) {
    super();
    mBase = base;
    mQuietTimeMs = quietTimeMs;
    mJournals = new ConcurrentHashMap<>();
    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.MASTER_UFS_JOURNAL_INITIAL_REPLAY_TIME_MS.getName(),
        () -> mInitialCatchupTimeMs);
    try {
      super.registerMetrics();
    } catch (RuntimeException e) {
      return;
    }
  }

  @Override
  public UfsJournal createJournal(Master master) {
    Supplier<Set<JournalSink>> supplier = () -> this.getJournalSinks(master);
    UfsJournal journal =
        new UfsJournal(URIUtils.appendPathOrDie(mBase, master.getName()), master, mQuietTimeMs,
            supplier);
    mJournals.put(master.getName(), journal);
    return journal;
  }

  @Override
  public void gainPrimacy() {
    List<Callable<Void>> callables = new ArrayList<>();
    for (Map.Entry<String, UfsJournal> entry : mJournals.entrySet()) {
      callables.add(() -> {
        UfsJournal journal = entry.getValue();
        journal.gainPrimacy();
        return null;
      });
    }
    try {
      CommonUtils.invokeAll(callables, 365L * Constants.DAY_MS);
    } catch (TimeoutException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void losePrimacy() {
    // Make all journals secondary as soon as possible
    for (UfsJournal journal : mJournals.values()) {
      journal.signalLosePrimacy();
    }

    // Wait for all journals to transition to secondary
    try {
      for (UfsJournal journal : mJournals.values()) {
        journal.awaitLosePrimacy();
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to downgrade journal to secondary", e);
    }
  }

  @Override
  public void suspend(Runnable interruptCallback) throws IOException {
    for (Map.Entry<String, UfsJournal> journalEntry : mJournals.entrySet()) {
      LOG.info("Suspending journal: {}", journalEntry.getKey());
      journalEntry.getValue().suspend();
    }
  }

  @Override
  public void resume() throws IOException {
    for (Map.Entry<String, UfsJournal> journalEntry : mJournals.entrySet()) {
      LOG.info("Resuming journal: {}", journalEntry.getKey());
      journalEntry.getValue().resume();
    }
  }

  @Override
  public CatchupFuture catchup(Map<String, Long> journalSequenceNumbers) throws IOException {
    List<CatchupFuture> futures = new ArrayList<>(journalSequenceNumbers.size());
    for (Map.Entry<String, UfsJournal> journalEntry : mJournals.entrySet()) {
      long resumeSequence = journalSequenceNumbers.get(journalEntry.getKey());
      LOG.info("Advancing journal :{} to sequence: {}", journalEntry.getKey(), resumeSequence);
      futures.add(journalEntry.getValue().catchup(resumeSequence));
    }
    return CatchupFuture.allOf(futures);
  }

  @Override
  public void waitForCatchup() {
    long start = System.currentTimeMillis();
    try (Timer.Context ctx = MetricsSystem
        .timer(MetricKey.MASTER_UFS_JOURNAL_CATCHUP_TIMER.getName()).time()) {
      CommonUtils.waitFor("journal catch up to finish", () -> {
        for (UfsJournal journal : mJournals.values()) {
          UfsJournalCheckpointThread.CatchupState catchupState = journal.getCatchupState();
          if (catchupState != UfsJournalCheckpointThread.CatchupState.DONE) {
            return false;
          }
        }
        return true;
      }, WaitForOptions.defaults().setTimeoutMs(
          (int) ServerConfiguration.getMs(PropertyKey.MASTER_UFS_JOURNAL_MAX_CATCHUP_TIME))
          .setInterval(Constants.SECOND_MS));
    } catch (InterruptedException | TimeoutException e) {
      LOG.info("Journal catchup is interrupted or timeout", e);
      if (mInitialCatchupTimeMs == -1) {
        mInitialCatchupTimeMs = System.currentTimeMillis() - start;
      }
      return;
    }
    if (mInitialCatchupTimeMs == -1) {
      mInitialCatchupTimeMs = System.currentTimeMillis() - start;
    }
    LOG.info("Finished master process ufs journal catchup in {} ms", mInitialCatchupTimeMs);
  }

  @Override
  public Map<String, Long> getCurrentSequenceNumbers() {
    Map<String, Long> sequenceMap = new HashMap<>();
    for (String master : mJournals.keySet()) {
      sequenceMap.put(master, mJournals.get(master).getNextSequenceNumberToWrite() - 1);
    }
    return sequenceMap;
  }

  @Override
  public void startInternal() throws IOException {
    for (UfsJournal journal : mJournals.values()) {
      journal.start();
    }
  }

  @Override
  public void stopInternal() {
    Closer closer = Closer.create();
    for (UfsJournal journal : mJournals.values()) {
      closer.register(journal);
    }
    RetryPolicy retry = ExponentialTimeBoundedRetry.builder()
        .withMaxDuration(Duration.ofMinutes(1))
        .withInitialSleep(Duration.ofMillis(100))
        .withMaxSleep(Duration.ofSeconds(3))
        .build();
    IOException exception = null;
    while (retry.attempt()) {
      try {
        closer.close();
        return;
      } catch (IOException e) {
        exception = e;
        LOG.warn("Failed to close journal: {}", e.toString());
      }
    }
    if (exception != null) {
      throw new RuntimeException(exception);
    }
  }

  @Override
  public boolean isFormatted() throws IOException {
    for (UfsJournal journal : mJournals.values()) {
      if (!journal.isFormatted()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public synchronized boolean isEmpty() {
    for (UfsJournal journal : mJournals.values()) {
      if (journal.getNextSequenceNumberToWrite() > 0) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void format() throws IOException {
    for (UfsJournal journal : mJournals.values()) {
      journal.format();
    }
  }

  @Override
  public void checkpoint() throws IOException {
    for (UfsJournal journal : mJournals.values()) {
      journal.checkpoint();
    }
  }
}
