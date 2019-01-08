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

package alluxio.master.journal.raft;

import alluxio.exception.status.UnavailableException;
import alluxio.master.journal.AsyncJournalWriter;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.MasterJournalContext;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.LockResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Lock;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Wrapper context around {@link MasterJournalContext} for holding the raft journal lock over the
 * lifetime of the journal context.
 */
@NotThreadSafe
public final class RaftJournalContext implements JournalContext {
  private static final Logger LOG = LoggerFactory.getLogger(RaftJournalContext.class);

  private final MasterJournalContext mContext;
  private final LockResource mLockResource;

  /**
   * Constructs a {@link MasterJournalContext}.
   *
   * @param asyncJournalWriter a {@link AsyncJournalWriter}
   * @param lock a lock to hold until the context is closed
   */
  public RaftJournalContext(AsyncJournalWriter asyncJournalWriter, Lock lock) {
    mLockResource = new LockResource(lock);
    mContext = new MasterJournalContext(asyncJournalWriter);
  }

  @Override
  public void append(JournalEntry entry) {
    mContext.append(entry);
  }

  @Override
  public void close() throws UnavailableException {
    try {
      mContext.close();
    } finally {
      // The journal must be flushed before we can release the lock.
      mLockResource.close();
    }
  }
}
