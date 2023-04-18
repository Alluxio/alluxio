package alluxio.master.metastore.rocks;

import static alluxio.master.metastore.rocks.RocksStoreTestUtils.waitForReaders;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.metastore.BlockMetaStore;
import alluxio.proto.meta.Block;
import alluxio.resource.CloseableIterator;
import alluxio.util.ThreadFactoryUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

public class RocksBlockMetaStoreTest {
  private static final int FILE_NUMBER = 400;
  private static final int THREAD_NUMBER = 20;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  public String mPath;
  public RocksBlockMetaStore mStore;

  private ExecutorService mThreadPool;

  @Before
  public void setUp() throws Exception {
    Configuration.set(PropertyKey.MASTER_METASTORE_ROCKS_EXCLUSIVE_LOCK_TIMEOUT, "500ms");
    Configuration.set(PropertyKey.TEST_MODE, true);
    // Wait for a shorter period of time in test
    Configuration.set(PropertyKey.MASTER_METASTORE_ROCKS_EXCLUSIVE_LOCK_TIMEOUT, "1s");
    mPath = mFolder.newFolder().getAbsolutePath();
    mStore = new RocksBlockMetaStore(mFolder.newFolder().getAbsolutePath());
    mThreadPool = Executors.newCachedThreadPool(ThreadFactoryUtils.build("test-executor-%d", true));
  }

  @After
  public void tearDown() throws Exception {
    mStore.close();
    mThreadPool.shutdownNow();
    mThreadPool = null;
  }

  @Test
  public void escapingIteratorExceptionInNext() throws Exception {
    prepareBlocks(FILE_NUMBER);

    FlakyRocksBlockStore delegateStore = new FlakyRocksBlockStore(mPath, mStore);
    AtomicReference<Exception> exception = new AtomicReference<>(null);
    try (CloseableIterator<BlockMetaStore.Block> brokenIter = delegateStore.getCloseableIterator(false, true)) {
      while (brokenIter.hasNext()) {
        brokenIter.next();
      }
    } catch (Exception e) {
      exception.set(e);
    }
    assertNotNull(exception.get());

    // Even if the iter is flaky, the lock and ref count are managed correctly
    // A close action will look at the ref count and err if there is a lock leak
    assertEquals(0, mStore.getRocksStore().getSharedLockCount());
    mStore.close();
  }

  @Test
  public void escapingIteratorExceptionInHasNext() throws Exception {
    prepareBlocks(FILE_NUMBER);

    FlakyRocksBlockStore delegateStore = new FlakyRocksBlockStore(mPath, mStore);
    AtomicReference<Exception> exception = new AtomicReference<>(null);
    try (CloseableIterator<BlockMetaStore.Block> brokenIter = delegateStore.getCloseableIterator(true, false)) {
      while (brokenIter.hasNext()) {
        brokenIter.next();
      }
    } catch (Exception e) {
      exception.set(e);
    }
    assertNotNull(exception.get());

    // Even if the iter is flaky, the lock and ref count are managed correctly
    // A close action will look at the ref count and err if there is a lock leak
    assertEquals(0, mStore.getRocksStore().getSharedLockCount());
    mStore.close();
  }

  @Test
  public void longRunningIterAndCheckpoint() throws Exception {
    // Manually set this flag, otherwise an exception will be thrown when the exclusive lock
    // is forced.
    Configuration.set(PropertyKey.TEST_MODE, false);
    prepareBlocks(FILE_NUMBER);

    // Create a bunch of long running iterators on the InodeStore
    CountDownLatch readerLatch = new CountDownLatch(THREAD_NUMBER);
    CountDownLatch restoreLatch = new CountDownLatch(1);
    ArrayBlockingQueue<Exception> errors = new ArrayBlockingQueue<>(THREAD_NUMBER);
    ArrayBlockingQueue<Integer> results = new ArrayBlockingQueue<>(THREAD_NUMBER);
    List<Future<Void>> futures = submitIterJob(THREAD_NUMBER, errors, results, readerLatch, restoreLatch);

    // Await for the 20 threads to be iterating in the middle, then trigger the shutdown event
    readerLatch.await();
    System.out.println("All 20 threads are running, shut down now");
    File checkpointFile = File.createTempFile("checkpoint-for-recovery", "");
    try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(checkpointFile))) {
      mStore.writeToCheckpoint(out);
    }
    System.out.println("Written checkpoint file with size " + Files.size(checkpointFile.toPath()));
    assertTrue(Files.size(checkpointFile.toPath()) > 0);

    // Verify that the iterators can still run
    System.out.println("Restore finished, let readers continue");
    restoreLatch.countDown();

    waitForReaders(futures);
    System.out.println("All threads finished on the new RocksDB");

    // All iterators should abort because the RocksDB contents have changed
    assertEquals(0, errors.size());
    System.out.println("Job status: " + results);
    long completed = results.stream().filter(n -> n == FILE_NUMBER).count();
    assertEquals(THREAD_NUMBER, completed);
  }

  @Test
  public void longRunningIterAndRestore() throws Exception {
    // Manually set this flag, otherwise an exception will be thrown when the exclusive lock
    // is forced.
    Configuration.set(PropertyKey.TEST_MODE, false);
    prepareBlocks(FILE_NUMBER);

    // Prepare a checkpoint file
    File checkpointFile = File.createTempFile("checkpoint-for-recovery", "");
    try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(checkpointFile))) {
      mStore.writeToCheckpoint(out);
    }
    System.out.println("Prepared checkpoint file with size " + Files.size(checkpointFile.toPath()));

    // Create a bunch of long running iterators on the InodeStore
    CountDownLatch readerLatch = new CountDownLatch(THREAD_NUMBER);
    CountDownLatch restoreLatch = new CountDownLatch(1);
    ArrayBlockingQueue<Exception> errors = new ArrayBlockingQueue<>(THREAD_NUMBER);
    ArrayBlockingQueue<Integer> results = new ArrayBlockingQueue<>(THREAD_NUMBER);
    List<Future<Void>> futures = submitIterJob(THREAD_NUMBER, errors, results, readerLatch, restoreLatch);

    // Await for the 20 threads to be iterating in the middle, then trigger the shutdown event
    readerLatch.await();
    System.out.println("All 20 threads are running, shut down now");
    try (CheckpointInputStream in = new CheckpointInputStream((new DataInputStream(new FileInputStream(checkpointFile))))) {
      mStore.restoreFromCheckpoint(in);
    }
    System.out.println("Restored from checkpoint");

    // Verify that the iterators can still run
    System.out.println("Restore finished, let readers continue");
    restoreLatch.countDown();

    waitForReaders(futures);
    System.out.println("All threads finished on the new RocksDB");

    // All iterators should abort because the RocksDB contents have changed
    assertEquals(THREAD_NUMBER, errors.size());
    System.out.println("Job status: " + results);
    long completed = results.stream().filter(n -> n == FILE_NUMBER).count();
    assertEquals(0, completed);
    long aborted = results.stream().filter(n -> n == 10).count();
    assertEquals(THREAD_NUMBER, aborted);
  }


  public static class FlakyRocksBlockStore extends RocksInodeStore {
    private final RocksBlockMetaStore mDelegate;

    public FlakyRocksBlockStore(String baseDir, RocksBlockMetaStore delegate) {
      super(baseDir);
      mDelegate = delegate;
    }

    public CloseableIterator<BlockMetaStore.Block> getCloseableIterator(boolean hasNextIsFlaky, boolean nextIsFlaky) {
      System.out.println("Get an iter from delegate");
      CloseableIterator<BlockMetaStore.Block> iter = mDelegate.getCloseableIterator();

      // This iterator is flaky
      return new CloseableIterator<BlockMetaStore.Block>(iter) {
        private int mCounter = 0;

        @Override
        public void closeResource() {
          iter.closeResource();
        }

        @Override
        public boolean hasNext() {
          System.out.println("hasNext");
          if (mCounter == 5 && hasNextIsFlaky) {
            throw new RuntimeException("Unexpected exception in iterator");
          }
          return iter.hasNext();
        }

        @Override
        public BlockMetaStore.Block next() {
          mCounter++;
          System.out.println("Step " + mCounter);
          if (mCounter == 5 && nextIsFlaky) {
            throw new RuntimeException("Unexpected exception in iterator");
          }
          return iter.next();
        }
      };
    }
  }

  private void prepareBlocks(int blockCount) throws Exception {
    for (int i = 1; i < blockCount + 1; i++) {
      mStore.putBlock(i, Block.BlockMeta.newBuilder().setLength(100).build());
    }
  }

  private List<Future<Void>> submitIterJob(int threadCount,
                                           ArrayBlockingQueue<Exception> errors,
                                           ArrayBlockingQueue<Integer> results,
                                           @Nullable CountDownLatch readersRunningLatch,
                                           @Nullable CountDownLatch writerCompletedLatch) {
    List<Future<Void>> futures = new ArrayList<>();
    for (int k = 0; k < threadCount; k++) {
      futures.add(mThreadPool.submit(() -> {
        int listedCount = 0;
        try (CloseableIterator<BlockMetaStore.Block> iter = mStore.getCloseableIterator()) {
          while (iter.hasNext()) {
            if (listedCount == 10 && readersRunningLatch != null) {
              readersRunningLatch.countDown();
              System.out.println("Reader is blocked and waits");
              if (writerCompletedLatch != null) {
                // Pretend the reader is blocked and will wake up after the writer is done
                writerCompletedLatch.await();
                System.out.println("Reader resumes after the writer is done");
              }
            }
            iter.next();
            listedCount++;
          }
        } catch (Exception e) {
          errors.add(e);
        } finally {
          results.add(listedCount);
          System.out.println("End - RocksStore has refCount=" + mStore.getRocksStore().getSharedLockCount());
        }
        return null;
      }));
    }
    return futures;
  }
}
