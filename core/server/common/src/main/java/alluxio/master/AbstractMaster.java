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

package alluxio.master;

import alluxio.Constants;
import alluxio.Server;
import alluxio.exception.status.UnavailableException;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.StateChangeJournalContext;
import alluxio.resource.LockResource;
import alluxio.util.executor.ExecutorServiceFactory;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This is the base class for all masters, and contains common functionality. Common functionality
 * mostly consists of journal operations, like initialization, journal tailing when in secondary
 * mode, or journal writing when the master is the primary.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1664)
public abstract class AbstractMaster implements Master {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractMaster.class);
  private static final long SHUTDOWN_TIMEOUT_MS = 10L * Constants.SECOND_MS;

  /** A factory for creating executor services when they are needed. */
  private ExecutorServiceFactory mExecutorServiceFactory;
  /** The executor used for running maintenance threads for the master. */
  private ExecutorService mExecutorService;
  /** A handler to the journal for this master. */
  private Journal mJournal;
  /** true if this master is in primary mode, and not secondary mode. */
  private boolean mIsPrimary = false;

  /** The clock to use for determining the time. */
  protected final Clock mClock;

  /** The context of Alluxio masters. **/
  protected final MasterContext mMasterContext;

  /**
   * @param masterContext the context for Alluxio master
   * @param clock the Clock to use for determining the time
   * @param executorServiceFactory a factory for creating the executor service to use for
   *        running maintenance threads
   */
  protected AbstractMaster(MasterContext masterContext, Clock clock,
      ExecutorServiceFactory executorServiceFactory) {
    Preconditions.checkNotNull(masterContext, "masterContext");
    mMasterContext = masterContext;
    mClock = clock;
    mExecutorServiceFactory = executorServiceFactory;
    mJournal = masterContext.getJournalSystem().createJournal(this);
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return new HashSet<>();
  }

  @Override
  public void start(Boolean isPrimary) throws IOException {
    Preconditions.checkState(mExecutorService == null);
    mExecutorService = mExecutorServiceFactory.create();
    mIsPrimary = isPrimary;
    if (mIsPrimary) {
      /**
       * The sequence for dealing with the journal before starting as the primary:
       *
       * 1. Replay the journal entries.
       * 2. Start the journal writer and optionally journal the master bootstrap states
       *    if this is a fresh start.
       *
       * Since this method is called before the master RPC server starts serving, there is no
       * concurrent access to the master during these phases.
       */

      LOG.info("{}: Starting primary master.", getName());
    }
  }

  @Override
  public void stop() throws IOException {
    if (mIsPrimary) {
      LOG.info("{}: Stopping primary master.", getName());
    }
    // Shut down the executor service, interrupting any running threads.
    if (mExecutorService != null) {
      try {
        mExecutorService.shutdownNow();
        String awaitFailureMessage =
            "waiting for {} executor service to shut down. Daemons may still be running";
        try {
          if (!mExecutorService.awaitTermination(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            LOG.warn("Timed out " + awaitFailureMessage, this.getClass().getSimpleName());
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOG.warn("Interrupted while " + awaitFailureMessage, this.getClass().getSimpleName());
        }
      } finally {
        mExecutorService = null;
      }
    }
    LOG.info("{}: Stopped {} master.", getName(), mIsPrimary ? "primary" : "secondary");
  }

  /**
   * @return the {@link ExecutorService} for this master
   */
  protected ExecutorService getExecutorService() {
    return mExecutorService;
  }

  @Override
  public JournalContext createJournalContext() throws UnavailableException {
    // Use the state change lock for the journal context, since all modifications to journaled
    // state must happen inside of a journal context.
    LockResource sharedLockResource;
    try {
      sharedLockResource = mMasterContext.getStateLockManager().lockShared();
    } catch (InterruptedException e) {
      throw new UnavailableException(
          "Failed to acquire state-lock due to ongoing backup activity.");
    }

    try {
      return new StateChangeJournalContext(mJournal.createJournalContext(), sharedLockResource);
    } catch (UnavailableException e) {
      sharedLockResource.close();
      throw e;
    }
  }

  @Override
  public void close() throws IOException {
    stop();
  }

  @Override
  public MasterContext getMasterContext() {
    return mMasterContext;
  }
}
