package alluxio.master.file;

import alluxio.exception.status.UnavailableException;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.NoopJournalContext;

import com.google.common.base.Throwables;

import java.io.Closeable;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Context passed through the span of a file system master RPC to manage actions that need to be
 * taken when the body of the RPC is finished.
 */
@NotThreadSafe
public final class RpcContext implements Closeable {
  public static final RpcContext NOOP =
      new RpcContext(NoopBlockDeletionContext.INSTANCE, NoopJournalContext.INSTANCE);

  @Nullable
  private final BlockDeletionContext mBlockDeletionContext;
  private final JournalContext mJournalContext;

  // Used during close to keep track of thrown exceptions.
  private Throwable mThrown = null;

  /**
   * Creates an {@link RpcContext}. This class aggregates different contexts used over the course of
   * an RPC, and makes sure they are closed in the right order when the RPC is finished.
   *
   * @param blockDeleter block deletion context
   * @param journalContext journal context
   */
  public RpcContext(BlockDeletionContext blockDeleter, JournalContext journalContext) {
    mBlockDeletionContext = blockDeleter;
    mJournalContext = journalContext;
  }

  /**
   * @return the journal context
   */
  public JournalContext getJournalContext() {
    return mJournalContext;
  }

  /**
   * @return the block deletion context
   */
  public BlockDeletionContext getBlockDeletionContext() {
    return mBlockDeletionContext;
  }

  @Override
  public void close() throws UnavailableException {
    // Order is important
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
}
