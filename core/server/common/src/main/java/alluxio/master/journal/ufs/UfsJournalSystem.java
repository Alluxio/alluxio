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
import alluxio.master.journal.AbstractJournalSystem;
import alluxio.master.journal.JournalEntryStateMachine;
import alluxio.retry.ExponentialTimeBoundedRetry;
import alluxio.retry.RetryPolicy;
import alluxio.util.CommonUtils;
import alluxio.util.URIUtils;

import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

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

  /**
   * Creates a UFS journal system with the specified base location. When journals are created, their
   * names are appended to the base location. The created journals all function independently.
   *
   * @param base the base location for journals created by this factory
   * @param quietTimeMs before upgrading from SECONDARY to PRIMARY mode, the journal will wait until
   *        this duration has passed without any journal entries being written.
   */
  public UfsJournalSystem(URI base, long quietTimeMs) {
    mBase = base;
    mQuietTimeMs = quietTimeMs;
    mJournals = new ConcurrentHashMap<>();
  }

  @Override
  public UfsJournal createJournal(JournalEntryStateMachine master) {
    UfsJournal journal =
        new UfsJournal(URIUtils.appendPathOrDie(mBase, master.getName()), master, mQuietTimeMs);
    mJournals.put(master.getName(), journal);
    return journal;
  }

  @Override
  public void gainPrimacy() {
    List<Callable<Void>> callables = new ArrayList<>();
    for (UfsJournal journal : mJournals.values()) {
      callables.add(() -> {
        journal.gainPrimacy();
        return null;
      });
    }
    try {
      CommonUtils.invokeAll(callables, 1 * Constants.HOUR_MS);
    } catch (TimeoutException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void losePrimacy() {
    try {
      for (UfsJournal journal : mJournals.values()) {
        journal.losePrimacy();
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to downgrade journal to secondary", e);
    }
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
}
