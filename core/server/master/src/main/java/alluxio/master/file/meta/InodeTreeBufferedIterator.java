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

package alluxio.master.file.meta;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.metastore.InodeStore;
import alluxio.proto.journal.Journal;
import alluxio.resource.CloseableIterator;
import alluxio.util.ThreadFactoryUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Used to iterate this InodeTree's state, while doing a read-ahead buffering. Traversal is done
 * concurrently and depth order for each branch is preserved in the final iteration order. This
 * makes applying those entries later more efficient by guaranteeing that a parent of an inode is
 * iterated before it.
 */
public class InodeTreeBufferedIterator implements Iterator<Journal.JournalEntry>, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(InodeTreeBufferedIterator.class);

  // Used to signal end of iteration.
  private static final long TERMINATION_SEQ = -1;
  private static final long FAILURE_SEQ = -2;
  private final ExecutorService mThreadPool;
  /** Underlying inode store. */
  InodeStore mInodeStore;
  /** Root inode for enumeration. */
  InodeDirectory mRootInode;
  /** Buffered entry queue. */
  BlockingQueue<Journal.JournalEntry> mEntryBuffer;
  /** Whether buffering is still running. */
  private AtomicBoolean mBufferingActive;
  /** Completion service for crawlers. */
  private CompletionService<Boolean> mCrawlerCompletionService;
  /** Active crawlers. */
  private Set<Future<?>> mActiveCrawlers;
  /** Executor for the coordinating thread. */
  private ExecutorService mCoordinatorExecutor;
  /** Directories for iterator threads to traverse. */
  private BlockingQueue<Inode> mDirectoriesToIterate;
  /** Used to keep the next element for the iteration. */
  private LinkedList<Journal.JournalEntry> mNextElements;
  /** For storing iteration failure. */
  private AtomicReference<Throwable> mBufferingFailure;

  /**
   * Creates buffered iterator with closeable interface.
   *
   * @param inodeStore the inode store
   * @param rootInode root inode
   * @return the buffered iterator
   */
  public static CloseableIterator<Journal.JournalEntry> create(InodeStore inodeStore,
      InodeDirectory rootInode) {
    InodeTreeBufferedIterator iterator = new InodeTreeBufferedIterator(inodeStore, rootInode);
    return new CloseableIterator<Journal.JournalEntry>(iterator) {
      @Override
      public void close() {
        iterator.close();
      }
    };
  }

  private InodeTreeBufferedIterator(InodeStore inodeStore, InodeDirectory rootInode) {
    mInodeStore = inodeStore;
    mRootInode = rootInode;
    // Initialize configuration values.
    int iteratorThreadCount =
        ServerConfiguration.getInt(PropertyKey.MASTER_METASTORE_INODE_ITERATION_CRAWLER_COUNT);
    int entryBufferSize =
        ServerConfiguration.getInt(PropertyKey.MASTER_METASTORE_INODE_ENUMERATOR_BUFFER_COUNT);

    // Create executors.
    mCoordinatorExecutor = Executors.newSingleThreadExecutor(
        ThreadFactoryUtils.build("inode-tree-crawler-coordinator-%d", true));
    mThreadPool = Executors.newFixedThreadPool(iteratorThreadCount,
        ThreadFactoryUtils.build("inode-tree-crawler-%d", true));
    mCrawlerCompletionService =
        new ExecutorCompletionService(mThreadPool);
    // Used to keep futures of active crawlers.
    mActiveCrawlers = new HashSet<>();

    // Using linked queue for fast insertion/removal from the queue.
    mEntryBuffer = new LinkedBlockingQueue<>(entryBufferSize);

    // Initialize directories to iterate.
    mDirectoriesToIterate = new LinkedBlockingQueue<>();
    if (mRootInode != null) {
      mDirectoriesToIterate.add(mRootInode);
    }

    // Initialize iteration buffers.
    mNextElements = new LinkedList<>();
    mBufferingFailure = new AtomicReference<>();

    // Start buffering entries by iteration.
    mBufferingActive = new AtomicBoolean(true);
    startBuffering();
  }

  /**
   * Starts buffering process by launching the coordinator task, which in turn spawns crawler tasks
   * for enumeration.
   */
  private void startBuffering() {
    /**
     * Runnable class that is used to branch out on a dir inode.
     */
    final class DirectoryCrawler implements Callable<Boolean> {
      // Dir to branch out.
      private Inode mDirInode;

      /**
       * Creates an instance for given dir inode.
       *
       * @param dirInode the dir inode
       */
      public DirectoryCrawler(Inode dirInode) {
        mDirInode = dirInode;
      }

      @Override
      public Boolean call() throws Exception {
        try {
          // Buffer current dir as JournalEntry.
          mEntryBuffer.put(mDirInode.toJournalEntry());

          // Enumerate on immediate children.
          Iterable<? extends Inode> children = mInodeStore.getChildren(mDirInode.asDirectory());
          children.forEach((child) -> {
            try {
              if (child.isDirectory()) {
                // Insert directory for further branching.
                mDirectoriesToIterate.put(child);
              } else {
                // Buffer current file as JournalEntry
                mEntryBuffer.put(child.toJournalEntry());
              }
            } catch (InterruptedException ie) {
              // Continue interrupt chain.
              Thread.currentThread().interrupt();
              throw new RuntimeException("Thread interrupted while enumerating a dir.");
            }
          });
          return true;
        } catch (InterruptedException ie) {
          // Continue interrupt chain.
          Thread.currentThread().interrupt();
          throw new RuntimeException("Thread interrupted while enumerating on a dir.");
        }
      }
    }

    // Create coordinator task.
    mCoordinatorExecutor.submit(() -> {
      try {
        // Loop as long as there is a dir to branch or there are active enumeration thread.
        while (mActiveCrawlers.size() > 0 || !mDirectoriesToIterate.isEmpty()) {
          if (!mDirectoriesToIterate.isEmpty()) {
            // There is a dir to enumerate.
            mActiveCrawlers.add(mCrawlerCompletionService
                .submit(new DirectoryCrawler(mDirectoriesToIterate.take())));
          } else {
            // No dirs but there are active threads.
            Future<?> crawlerFuture = mCrawlerCompletionService.poll(100, TimeUnit.MILLISECONDS);
            if (crawlerFuture != null) {
              mActiveCrawlers.remove(crawlerFuture);
              crawlerFuture.get();
            }
          }
        }
        // Signal end of buffering.
        mEntryBuffer
            .put(Journal.JournalEntry.newBuilder().setSequenceNumber(TERMINATION_SEQ).build());
      } catch (InterruptedException ie) {
        // Cancel pending crawlers.
        mActiveCrawlers.forEach((future) -> future.cancel(true));
        // Continue interrupt chain.
        Thread.currentThread().interrupt();
        throw new RuntimeException("Thread interrupted while waiting for enumeration threads.");
      } catch (ExecutionException ee) {
        // Cancel pending crawlers.
        mActiveCrawlers.forEach((future) -> future.cancel(true));
        LOG.error("InodeTree buffering stopped due to crawler thread failure.", ee.getCause());
        mBufferingFailure.set(ee.getCause());
        // Signal failure during buffering.
        try {
          mEntryBuffer
              .put(Journal.JournalEntry.newBuilder().setSequenceNumber(FAILURE_SEQ).build());
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Thread interrupted while signaling for buffering failure.");
        }
      } finally {
        // Signal completion of buffering.
        mBufferingActive.set(false);
      }
    });
    // Will be shutdown after current coordinator task is complete.
    mCoordinatorExecutor.shutdown();
  }

  @Override
  public boolean hasNext() {
    /*
     * This call returns {@code true) if it was able to fetch an element from an ongoing buffering.
     * Then it stores that element to be remembered in following calls to hasNext() and next().
     *
     * Fetching an item from buffering requires synchronization, so this call is blocked until an
     * entry is retrieved or buffering is completed with no entries.
     */

    // Check for termination entry.
    // This assumes the termination entry will be enqueued the last.
    if (mNextElements.size() == 1
        && mNextElements.peekFirst().getSequenceNumber() == TERMINATION_SEQ) {
      return false;
    }

    if (mNextElements.size() > 0) {
      // hasNext() has been called before and cached some elements.
      // next() can return the next element.
      return true;
    }

    // Continue until taking an entry or failing due to having no entry.
    while (true) {
      // Return {@code false} if buffering is completed but still no entries.
      if (!mBufferingActive.get() && mEntryBuffer.size() == 0) {
        return false;
      }
      try {
        // Drain existing elements.
        if (0 == mEntryBuffer.drainTo(mNextElements)) {
          // Fall back to polling.
          Journal.JournalEntry entry;
          while (mBufferingActive.get() || mEntryBuffer.size() > 0) {
            // Poll in-case buffering is failed without proper termination.
            entry = mEntryBuffer.poll(30, TimeUnit.SECONDS);
            if (entry != null) {
              mNextElements.addLast(entry);
              break;
            }
          }
        }
        // Return true if there are more entries,
        // unless there is only a single termination entry.
        if (mNextElements.size() == 1) {
          return mNextElements.peekLast().getSequenceNumber() != TERMINATION_SEQ;
        } else {
          return mNextElements.size() > 0;
        }
      } catch (InterruptedException ie) {
        mActiveCrawlers.forEach((future) -> future.cancel(true));
        // Continue interrupt chain.
        Thread.currentThread().interrupt();
        throw new RuntimeException("Thread interrupted while taking an entry from buffer.");
      }
    }
  }

  @Override
  public Journal.JournalEntry next() {
    if (mNextElements.size() == 0) {
      // It's either:
      // -> next() has been called without a preceding hasNext().
      // -> there is no next entry.
      //
      // Call hasNext() to understand which one of the above is true.
      //
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      // hasNext() has been called before and stored the next element.
      // Return and reset state for the next item.
    }
    // Store next element.
    Journal.JournalEntry nextElement = mNextElements.removeFirst();
    // Throw failure if current element is a signal for failure.
    if (nextElement.getSequenceNumber() == FAILURE_SEQ) {
      throw new RuntimeException(mBufferingFailure.get());
    }

    return nextElement;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove is not supported in inode tree iterator");
  }

  @Override
  public void close() {
    LOG.debug("Closing {} inode tree iterators", mActiveCrawlers.size());
    mCoordinatorExecutor.shutdownNow();
    mThreadPool.shutdownNow();
  }
}
