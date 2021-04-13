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

package alluxio.master.file;

import alluxio.exception.status.UnavailableException;
import alluxio.master.file.contexts.CallTracker;
import alluxio.master.file.contexts.InternalOperationContext;
import alluxio.master.file.contexts.OperationContext;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.NoopJournalContext;
import alluxio.proto.journal.Journal.JournalEntry;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Context passed through the span of a file system master RPC to manage actions that need to be
 * taken when the body of the RPC is finished.
 *
 * Note that we cannot use Guava's {@link com.google.common.io.Closer} because it doesn't make
 * guarantees about the order in which resources are closed.
 */
@NotThreadSafe
public final class RpcContext implements Closeable, Supplier<JournalContext> {
  private static final Logger LOG = LoggerFactory.getLogger(RpcContext.class);
  public static final RpcContext NOOP = new RpcContext(NoopBlockDeletionContext.INSTANCE,
      NoopJournalContext.INSTANCE, new InternalOperationContext());

  @Nullable
  private final BlockDeletionContext mBlockDeletionContext;
  private final JournalContext mJournalContext;
  private final OperationContext mOperationContext;

  // Used during close to keep track of thrown exceptions.
  private Throwable mThrown = null;

  /**
   * Creates an {@link RpcContext}. This class aggregates different contexts used over the course of
   * an RPC, and makes sure they are closed in the right order when the RPC is finished.
   *
   * @param blockDeleter block deletion context
   * @param journalContext journal context
   * @param operationContext the operation context
   */
  public RpcContext(BlockDeletionContext blockDeleter, JournalContext journalContext,
      OperationContext operationContext) {
    mBlockDeletionContext = blockDeleter;
    mJournalContext = journalContext;
    mOperationContext = operationContext;
  }

  /**
   * @return the journal context
   */
  public JournalContext getJournalContext() {
    return mJournalContext;
  }

  /**
   * Syntax sugar for getJournalContext().append(entry).
   *
   * @param entry the {@link JournalEntry} to append to the journal
   */
  public void journal(JournalEntry entry) {
    mJournalContext.append(entry);
  }

  /**
   * @return the block deletion context
   */
  public BlockDeletionContext getBlockDeletionContext() {
    return mBlockDeletionContext;
  }

  /**
   * @return the operation context
   */
  public OperationContext getOperationContext() {
    return mOperationContext;
  }

  /**
   * Throws {@link RuntimeException} if the RPC was cancelled by any tracker.
   */
  public void throwIfCancelled() {
    List<CallTracker> cancelledTrackers = mOperationContext.getCancelledTrackers();
    if (cancelledTrackers.size() > 0) {
      throw new RuntimeException(String.format("Call cancelled by trackers: %s", cancelledTrackers
          .stream().map((t) -> t.getType().name()).collect(Collectors.joining(", "))));
    }
  }

  /**
   * @return {@code true} if the operation was cancelled
   */
  public boolean isCancelled() {
    return mOperationContext.getCancelledTrackers().size() > 0;
  }

  @Override
  public void close() throws UnavailableException {
    // JournalContext is closed before block deletion context so that file system master changes
    // are written before block master changes. If a failure occurs between deleting an inode and
    // remove its blocks, it's better to have an orphaned block than an inode with a missing block.
    closeQuietly(mJournalContext);
    closeQuietly(mBlockDeletionContext);

    if (mThrown != null) {
      Throwables.propagateIfPossible(mThrown, UnavailableException.class);
      throw new RuntimeException(mThrown);
    }
  }

  private void closeQuietly(AutoCloseable c) {
    if (c != null) {
      try {
        c.close();
      } catch (Throwable t) {
        if (mThrown != null) {
          mThrown.addSuppressed(t);
        } else {
          mThrown = t;
        }
      }
    }
  }

  @Override
  public JournalContext get() {
    return mJournalContext;
  }
}
