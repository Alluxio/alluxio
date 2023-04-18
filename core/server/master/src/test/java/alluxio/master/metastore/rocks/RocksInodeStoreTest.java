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

package alluxio.master.metastore.rocks;

import static alluxio.master.metastore.rocks.RocksStoreTestUtils.waitForReaders;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.meta.InodeView;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.file.meta.MutableInodeDirectory;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.metastore.InodeStore.WriteBatch;
import alluxio.master.metastore.ReadOption;
import alluxio.resource.CloseableIterator;
import alluxio.util.ThreadFactoryUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import javax.annotation.Nullable;

public class RocksInodeStoreTest {
  private static final int FILE_NUMBER = 400;
  private static final int THREAD_NUMBER = 20;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  public String mPath;
  public RocksInodeStore mStore;

  private ExecutorService mThreadPool;

  // Functional wrappers of RocksDB r/w actions
  private QuadFunction<ArrayBlockingQueue<Exception>, ArrayBlockingQueue<Integer>,
          CountDownLatch, CountDownLatch, List<Future<Void>>> mCreateAddReaders =
              (errors, results, readerRunningLatch, writerCompletedLatch) -> {
                return submitAddInodeJob(
                        errors, results, readerRunningLatch, writerCompletedLatch);
              };
  private QuadFunction<ArrayBlockingQueue<Exception>, ArrayBlockingQueue<Integer>,
          CountDownLatch, CountDownLatch, List<Future<Void>>> mCreateGetReaders =
              (errors, results, readerRunningLatch, writerCompletedLatch) -> {
                return submitGetInodeJob(
                        errors, results, readerRunningLatch, writerCompletedLatch);
              };
  private QuadFunction<ArrayBlockingQueue<Exception>, ArrayBlockingQueue<Integer>,
          CountDownLatch, CountDownLatch, List<Future<Void>>> mCreateListReadersAbort =
              (errors, results, readerRunningLatch, writerCompletedLatch) -> {
                // Do not wait for the writer latch, writer will run concurrent to the list actions
                return submitListingJob(
                        errors, results, readerRunningLatch, null);
              };

  @Before
  public void setUp() throws Exception {
    Configuration.set(PropertyKey.MASTER_METASTORE_ROCKS_EXCLUSIVE_LOCK_TIMEOUT, "500ms");
    Configuration.set(PropertyKey.TEST_MODE, true);
    // Wait for a shorter period of time in test
    Configuration.set(PropertyKey.MASTER_METASTORE_ROCKS_EXCLUSIVE_LOCK_TIMEOUT, "1s");
    mPath = mFolder.newFolder().getAbsolutePath();
    mStore = new RocksInodeStore(mFolder.newFolder().getAbsolutePath());
    mThreadPool = Executors.newCachedThreadPool(
        ThreadFactoryUtils.build("test-executor-%d", true));
  }

  @After
  public void tearDown() throws Exception {
    mStore.close();
    mThreadPool.shutdownNow();
    mThreadPool = null;
  }

  @Test
  public void batchWrite() throws IOException {
    WriteBatch batch = mStore.createWriteBatch();
    for (int i = 1; i < 20; i++) {
      batch.writeInode(
          MutableInodeDirectory.create(i, 0, "dir" + i, CreateDirectoryContext.defaults()));
    }
    batch.commit();
    for (int i = 1; i < 20; i++) {
      assertEquals("dir" + i, mStore.get(i).get().getName());
    }
  }

  @Test
  public void toStringEntries() throws IOException {
    assertEquals("", mStore.toStringEntries());

    mStore.writeInode(MutableInodeDirectory.create(
        1, 0, "dir", CreateDirectoryContext.defaults()));
    assertEquals("dir", mStore.get(1).get().getName());
    assertThat(mStore.toStringEntries(), containsString("name=dir"));
  }

  @Test
  public void concurrentListAndClose() throws Exception {
    testConcurrentReaderAndClose(mCreateListReadersAbort);
  }

  @Test
  public void concurrentListAndRestore() throws Exception {
    testConcurrentReaderAndRestore(mCreateListReadersAbort, (errors, results) -> {
      assertTrue(errors.size() <= THREAD_NUMBER);
      // Depending on the thread execution order, sometimes the reader threads
      // may run to finish before the writer thread picks up the signal and flag
      long completed = results.stream().filter(n -> n == FILE_NUMBER).count();
      assertEquals(THREAD_NUMBER, completed + errors.size());
      return null;
    }, (errors, results) -> {
      // Results are all empty after the clear
      assertEquals(0, errors.size());
      long completed = results.stream().filter(n -> n == FILE_NUMBER).count();
      assertEquals(THREAD_NUMBER, completed);
      return null;
    });
  }

  @Test
  public void concurrentListAndCheckpoint() throws Exception {
    testConcurrentReaderAndCheckpoint(mCreateListReadersAbort, (errors, results) -> {
      assertTrue(errors.size() <= THREAD_NUMBER);
      // Depending on the thread execution order, sometimes the reader threads
      // may run to finish before the writer thread picks up the signal and flag
      long completed = results.stream().filter(n -> n == FILE_NUMBER).count();
      assertEquals(THREAD_NUMBER, completed + errors.size());
      return null;
    }, (errors, results) -> {
      assertEquals(0, errors.size());
      long completed = results.stream().filter(n -> n == FILE_NUMBER).count();
      assertEquals(THREAD_NUMBER, completed);
      return null;
    });
  }

  @Test
  public void concurrentListAndClear() throws Exception {
    testConcurrentReaderAndClear(mCreateListReadersAbort, (errors, results) -> {
      assertTrue(errors.size() <= THREAD_NUMBER);
      // Depending on the thread execution order, sometimes the reader threads
      // may run to finish before the writer thread picks up the signal and flag
      long completed = results.stream().filter(n -> n == FILE_NUMBER).count();
      assertEquals(THREAD_NUMBER, completed + errors.size());
      return null;
    }, (errors, results) -> {
      // Results are all empty after the clear
      assertEquals(0, errors.size());
      long seeEmpty = results.stream().filter(n -> n == 0).count();
      assertEquals(THREAD_NUMBER, seeEmpty);
      return null;
    });
  }

  @Test
  public void concurrentGetAndClose() throws Exception {
    testConcurrentReaderAndClose(mCreateGetReaders);
  }

  @Test
  public void concurrentGetAndRestore() throws Exception {
    testConcurrentReaderAndRestore(mCreateGetReaders, (errors, results) -> {
      // The closer will finish and the new Get operations are unaffected
      // If one inode does not exist, result will be Optional.empty
      assertEquals(0, errors.size());
      long completed = results.stream().filter(n -> n == THREAD_NUMBER).count();
      assertEquals(THREAD_NUMBER, completed);
      return null;
    }, (errors, results) -> {
      assertEquals(0, errors.size());
      long completed = results.stream().filter(n -> n == THREAD_NUMBER).count();
      assertEquals(THREAD_NUMBER, completed);
      return null;
    });
  }

  @Test
  public void concurrentGetAndCheckpoint() throws Exception {
    testConcurrentReaderAndCheckpoint(mCreateGetReaders, (errors, results) -> {
      // The closer will finish and the new Get operations are unaffected
      assertEquals(0, errors.size());
      long completed = results.stream().filter(n -> n == THREAD_NUMBER).count();
      assertEquals(THREAD_NUMBER, completed);
      return null;
    }, (errors, results) -> {
      assertEquals(0, errors.size());
      long completed = results.stream().filter(n -> n == THREAD_NUMBER).count();
      assertEquals(THREAD_NUMBER, completed);
      return null;
    });
  }

  @Test
  public void concurrentGetAndClear() throws Exception {
    testConcurrentReaderAndClear(mCreateGetReaders, (errors, results) -> {
      // The closer will finish and the new Get operations are unaffected
      // However, Get after the RocksDB is cleared will get empty results
      assertEquals(0, errors.size());
      long completed = results.stream().filter(n -> n == THREAD_NUMBER).count();
      assertEquals(THREAD_NUMBER, completed);
      return null;
    }, (errors, results) -> {
      assertEquals(0, errors.size());
      long completed = results.stream().filter(n -> n == THREAD_NUMBER).count();
      assertEquals(THREAD_NUMBER, completed);
      return null;
    });
  }

  @Test
  public void concurrentAddAndClose() throws Exception {
    testConcurrentReaderAndClose(mCreateAddReaders);
  }

  @Test
  public void concurrentAddAndRestore() throws Exception {
    testConcurrentReaderAndRestore(mCreateAddReaders, (errors, results) -> {
      // After the restore finishes, new add operations can go on unaffected
      assertEquals(0, errors.size());
      long completed = results.stream().filter(n -> n == THREAD_NUMBER).count();
      assertEquals(THREAD_NUMBER, completed);
      return null;
    }, (errors, results) -> {
      assertEquals(0, errors.size());
      long completed = results.stream().filter(n -> n == THREAD_NUMBER).count();
      assertEquals(THREAD_NUMBER, completed);
      return null;
    });
  }

  @Test
  public void concurrentAddAndCheckpoint() throws Exception {
    testConcurrentReaderAndCheckpoint(mCreateAddReaders, (errors, results) -> {
      // After the clear finishes, add operations can go on unaffected
      assertEquals(0, errors.size());
      long completed = results.stream().filter(n -> n == THREAD_NUMBER).count();
      assertEquals(THREAD_NUMBER, completed);
      return null;
    }, (errors, results) -> {
      assertEquals(0, errors.size());
      long completed = results.stream().filter(n -> n == THREAD_NUMBER).count();
      assertEquals(THREAD_NUMBER, completed);
      return null;
    });
  }

  @Test
  public void concurrentAddAndClear() throws Exception {
    testConcurrentReaderAndClear(mCreateAddReaders, (errors, results) -> {
      // After the clear finishes, add operations can go on unaffected
      assertEquals(0, errors.size());
      long completed = results.stream().filter(n -> n == THREAD_NUMBER).count();
      assertEquals(THREAD_NUMBER, completed);
      return null;
    }, (errors, results) -> {
      assertEquals(0, errors.size());
      long completed = results.stream().filter(n -> n == THREAD_NUMBER).count();
      assertEquals(THREAD_NUMBER, completed);
      return null;
    });
  }

  private List<Future<Void>> submitListingJob(
          ArrayBlockingQueue<Exception> errors,
          ArrayBlockingQueue<Integer> results,
          @Nullable CountDownLatch readersRunningLatch,
          @Nullable CountDownLatch writerCompletedLatch) {
    List<Future<Void>> futures = new ArrayList<>();
    for (int k = 0; k < THREAD_NUMBER; k++) {
      futures.add(mThreadPool.submit(() -> {
        int listedCount = 0;
        try (CloseableIterator<Long> iter = mStore.getChildIds(0L)) {
          while (iter.hasNext()) {
            if (listedCount == 10 && readersRunningLatch != null) {
              readersRunningLatch.countDown();
              if (writerCompletedLatch != null) {
                // Pretend the reader is blocked and will wake up after the writer is done
                writerCompletedLatch.await();
              }
            }
            iter.next();
            listedCount++;
          }
        } catch (Exception e) {
          errors.add(e);
        } finally {
          results.add(listedCount);
        }
        return null;
      }));
    }
    return futures;
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
        try (CloseableIterator<InodeView> iter = mStore.getCloseableIterator()) {
          while (iter.hasNext()) {
            if (listedCount == 10 && readersRunningLatch != null) {
              readersRunningLatch.countDown();
              if (writerCompletedLatch != null) {
                // Pretend the reader is blocked and will wake up after the writer is done
                writerCompletedLatch.await();
              }
            }
            iter.next();
            listedCount++;
          }
        } catch (Exception e) {
          errors.add(e);
        } finally {
          results.add(listedCount);
        }
        return null;
      }));
    }
    return futures;
  }

  @Test
  public void escapingIteratorExceptionInNext() throws Exception {
    prepareFiles(FILE_NUMBER);

    FlakyRocksInodeStore delegateStore = new FlakyRocksInodeStore(mPath, mStore);
    AtomicReference<Exception> exception = new AtomicReference<>(null);
    try (CloseableIterator<InodeView> brokenIter =
             delegateStore.getCloseableIterator(false, true)) {
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
    prepareFiles(FILE_NUMBER);

    FlakyRocksInodeStore delegateStore = new FlakyRocksInodeStore(mPath, mStore);
    AtomicReference<Exception> exception = new AtomicReference<>(null);
    try (CloseableIterator<InodeView> brokenIter =
             delegateStore.getCloseableIterator(true, false)) {
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
  public void longRunningIterAndRestore() throws Exception {
    // Manually set this flag, otherwise an exception will be thrown when the exclusive lock
    // is forced.
    Configuration.set(PropertyKey.TEST_MODE, false);
    prepareFiles(FILE_NUMBER);

    // Prepare a checkpoint file
    File checkpointFile = File.createTempFile("checkpoint-for-recovery", "");
    try (BufferedOutputStream out =
             new BufferedOutputStream(new FileOutputStream(checkpointFile))) {
      mStore.writeToCheckpoint(out);
    }

    // Create a bunch of long running iterators on the InodeStore
    CountDownLatch readerLatch = new CountDownLatch(THREAD_NUMBER);
    CountDownLatch restoreLatch = new CountDownLatch(1);
    ArrayBlockingQueue<Exception> errors = new ArrayBlockingQueue<>(THREAD_NUMBER);
    ArrayBlockingQueue<Integer> results = new ArrayBlockingQueue<>(THREAD_NUMBER);
    List<Future<Void>> futures =
        submitIterJob(THREAD_NUMBER, errors, results, readerLatch, restoreLatch);

    // Await for the 20 threads to be iterating in the middle, then trigger the shutdown event
    readerLatch.await();
    try (CheckpointInputStream in = new CheckpointInputStream(
        (new DataInputStream(new FileInputStream(checkpointFile))))) {
      mStore.restoreFromCheckpoint(in);
    }

    // Verify that the iterators can still run
    restoreLatch.countDown();
    waitForReaders(futures);

    // All iterators should abort because the RocksDB contents have changed
    assertEquals(THREAD_NUMBER, errors.size());
    long completed = results.stream().filter(n -> n == FILE_NUMBER).count();
    assertEquals(0, completed);
    long aborted = results.stream().filter(n -> n == 10).count();
    assertEquals(THREAD_NUMBER, aborted);
  }

  @Test
  public void longRunningIterAndCheckpoint() throws Exception {
    // Manually set this flag, otherwise an exception will be thrown when the exclusive lock
    // is forced.
    Configuration.set(PropertyKey.TEST_MODE, false);
    prepareFiles(FILE_NUMBER);

    // Create a bunch of long running iterators on the InodeStore
    CountDownLatch readerLatch = new CountDownLatch(THREAD_NUMBER);
    CountDownLatch restoreLatch = new CountDownLatch(1);
    ArrayBlockingQueue<Exception> errors = new ArrayBlockingQueue<>(THREAD_NUMBER);
    ArrayBlockingQueue<Integer> results = new ArrayBlockingQueue<>(THREAD_NUMBER);
    List<Future<Void>> futures =
        submitIterJob(THREAD_NUMBER, errors, results, readerLatch, restoreLatch);

    // Await for the 20 threads to be iterating in the middle, then trigger the shutdown event
    readerLatch.await();
    File checkpointFile = File.createTempFile("checkpoint-for-recovery", "");
    try (BufferedOutputStream out =
             new BufferedOutputStream(new FileOutputStream(checkpointFile))) {
      mStore.writeToCheckpoint(out);
    }
    assertTrue(Files.size(checkpointFile.toPath()) > 0);

    // Verify that the iterators can still run
    restoreLatch.countDown();
    waitForReaders(futures);

    // All iterators should abort because the RocksDB contents have changed
    assertEquals(0, errors.size());
    long completed = results.stream().filter(n -> n == FILE_NUMBER).count();
    assertEquals(THREAD_NUMBER, completed);
  }

  public static class FlakyRocksInodeStore extends RocksInodeStore {
    private final RocksInodeStore mDelegate;

    public FlakyRocksInodeStore(String baseDir, RocksInodeStore delegate) {
      super(baseDir);
      mDelegate = delegate;
    }

    public CloseableIterator<InodeView> getCloseableIterator(
            boolean hasNextIsFlaky, boolean nextIsFlaky) {
      CloseableIterator<InodeView> iter = mDelegate.getCloseableIterator();

      // This iterator is flaky
      return new CloseableIterator<InodeView>(iter) {
        private int mCounter = 0;

        @Override
        public void closeResource() {
          iter.closeResource();
        }

        @Override
        public boolean hasNext() {
          if (mCounter == 5 && hasNextIsFlaky) {
            throw new RuntimeException("Unexpected exception in iterator");
          }
          return iter.hasNext();
        }

        @Override
        public InodeView next() {
          mCounter++;
          if (mCounter == 5 && nextIsFlaky) {
            throw new RuntimeException("Unexpected exception in iterator");
          }
          return iter.next();
        }
      };
    }
  }

  private List<Future<Void>> submitGetInodeJob(
          ArrayBlockingQueue<Exception> errors,
          ArrayBlockingQueue<Integer> results,
          @Nullable CountDownLatch readersRunningLatch,
          @Nullable CountDownLatch writerCompletedLatch) {
    List<Future<Void>> futures = new ArrayList<>();
    for (int k = 0; k < THREAD_NUMBER; k++) {
      final int iterNum = k;
      futures.add(mThreadPool.submit(() -> {
        int finishedCount = 0;
        try {
          for (int x = 0; x < THREAD_NUMBER; x++) {
            long targetInodeId = iterNum * THREAD_NUMBER + x;
            Optional<MutableInode<?>> dir = mStore.getMutable(targetInodeId, ReadOption.defaults());
            finishedCount++;
            if (x == 10 && readersRunningLatch != null) {
              readersRunningLatch.countDown();
              if (writerCompletedLatch != null) {
                // Pretend the reader is blocked and will wake up after the writer is done
                writerCompletedLatch.await();
              }
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
          errors.add(e);
        } finally {
          results.add(finishedCount);
        }
        return null;
      }));
    }
    return futures;
  }

  private List<Future<Void>> submitAddInodeJob(
          ArrayBlockingQueue<Exception> errors,
          ArrayBlockingQueue<Integer> results,
          @Nullable CountDownLatch readersRunningLatch,
          @Nullable CountDownLatch writerCompletedLatch) {
    List<Future<Void>> futures = new ArrayList<>();
    for (int k = 0; k < THREAD_NUMBER; k++) {
      final int iterNum = k;
      futures.add(mThreadPool.submit(() -> {
        int finishedCount = 0;
        try {
          for (int x = 0; x < THREAD_NUMBER; x++) {
            long targetInodeId = iterNum * THREAD_NUMBER + x;
            MutableInodeDirectory dir =
                MutableInodeDirectory.create(targetInodeId, 0, "dir" + targetInodeId,
                    CreateDirectoryContext.defaults());
            mStore.addChild(0L, dir);
            if (x == 10 && readersRunningLatch != null) {
              readersRunningLatch.countDown();
              if (writerCompletedLatch != null) {
                // Pretend the reader is blocked and will wake up after the writer is done
                writerCompletedLatch.await();
              }
            }
            finishedCount++;
          }
        } catch (Exception e) {
          errors.add(e);
        } finally {
          results.add(finishedCount);
        }
        return null;
      }));
    }
    return futures;
  }

  private void testConcurrentReaderAndClose(
      QuadFunction<ArrayBlockingQueue<Exception>, ArrayBlockingQueue<Integer>, CountDownLatch,
          CountDownLatch, List<Future<Void>>> reader) throws Exception {
    prepareFiles(FILE_NUMBER);

    CountDownLatch readerRunningLatch = new CountDownLatch(THREAD_NUMBER);
    CountDownLatch writerCompletedLatch = new CountDownLatch(1);
    ArrayBlockingQueue<Exception> errors = new ArrayBlockingQueue<>(THREAD_NUMBER);
    ArrayBlockingQueue<Integer> results = new ArrayBlockingQueue<>(THREAD_NUMBER);
    List<Future<Void>> futures =
        reader.apply(errors, results, readerRunningLatch, writerCompletedLatch);

    // Await for the threads to be running in the middle, then trigger the closer event
    readerRunningLatch.await();
    mStore.close();
    writerCompletedLatch.countDown();

    waitForReaders(futures);
    // Reaching here means close() was successfully, which implies ref count reached zero
    assertTrue(errors.size() <= THREAD_NUMBER);
  }

  private void testConcurrentReaderAndCheckpoint(
      QuadFunction<ArrayBlockingQueue<Exception>, ArrayBlockingQueue<Integer>, CountDownLatch,
          CountDownLatch, List<Future<Void>>> reader,
      BiFunction<ArrayBlockingQueue<Exception>, ArrayBlockingQueue<Integer>,
          Void> stateCheckAfterReadersFinish,
      BiFunction<ArrayBlockingQueue<Exception>, ArrayBlockingQueue<Integer>,
          Void> stateCheckAfterReadersFinishAgain
  ) throws Exception {
    prepareFiles(FILE_NUMBER);

    CountDownLatch readerRunningLatch = new CountDownLatch(THREAD_NUMBER);
    CountDownLatch writerCompletedLatch = new CountDownLatch(1);
    ArrayBlockingQueue<Exception> errors = new ArrayBlockingQueue<>(THREAD_NUMBER);
    ArrayBlockingQueue<Integer> results = new ArrayBlockingQueue<>(THREAD_NUMBER);
    List<Future<Void>> futures =
        reader.apply(errors, results, readerRunningLatch, writerCompletedLatch);

    // Await for the 20 threads to be iterating in the middle, then trigger the shutdown event
    readerRunningLatch.await();
    File checkpointFile = File.createTempFile("checkpoint-file", "");
    try (BufferedOutputStream out =
             new BufferedOutputStream(new FileOutputStream(checkpointFile))) {
      mStore.writeToCheckpoint(out);
    }
    assertTrue(Files.size(checkpointFile.toPath()) > 0);
    writerCompletedLatch.countDown();

    waitForReaders(futures);
    stateCheckAfterReadersFinish.apply(errors, results);

    // Verify that the RocksDB can still serve
    ArrayBlockingQueue<Exception> errorsAgain = new ArrayBlockingQueue<>(THREAD_NUMBER);
    ArrayBlockingQueue<Integer> resultsAgain = new ArrayBlockingQueue<>(THREAD_NUMBER);
    List<Future<Void>> futuresAgain = reader.apply(errorsAgain, resultsAgain, null, null);
    waitForReaders(futuresAgain);
    stateCheckAfterReadersFinishAgain.apply(errorsAgain, resultsAgain);
  }

  private void testConcurrentReaderAndRestore(
      QuadFunction<ArrayBlockingQueue<Exception>, ArrayBlockingQueue<Integer>,
          CountDownLatch, CountDownLatch, List<Future<Void>>> reader,
      BiFunction<ArrayBlockingQueue<Exception>, ArrayBlockingQueue<Integer>,
          Void> stateCheckAfterReadersFinish,
      BiFunction<ArrayBlockingQueue<Exception>, ArrayBlockingQueue<Integer>,
          Void> stateCheckAfterReadersFinishAgain
  ) throws Exception {
    prepareFiles(FILE_NUMBER);
    // Prepare a checkpoint file
    File checkpointFile = File.createTempFile("checkpoint-for-recovery", "");
    try (BufferedOutputStream out =
             new BufferedOutputStream(new FileOutputStream(checkpointFile))) {
      mStore.writeToCheckpoint(out);
    }

    CountDownLatch readerRunningLatch = new CountDownLatch(THREAD_NUMBER);
    CountDownLatch writerCompletedLatch = new CountDownLatch(1);
    ArrayBlockingQueue<Exception> errors = new ArrayBlockingQueue<>(THREAD_NUMBER);
    ArrayBlockingQueue<Integer> results = new ArrayBlockingQueue<>(THREAD_NUMBER);
    List<Future<Void>> futures =
        reader.apply(errors, results, readerRunningLatch, writerCompletedLatch);

    // Await for the 20 threads to be iterating in the middle, then trigger the shutdown event
    readerRunningLatch.await();
    try (CheckpointInputStream in = new CheckpointInputStream(
        (new DataInputStream(new FileInputStream(checkpointFile))))) {
      mStore.restoreFromCheckpoint(in);
    }
    writerCompletedLatch.countDown();
    waitForReaders(futures);
    stateCheckAfterReadersFinish.apply(errors, results);

    // Verify that the RocksDB can still serve
    ArrayBlockingQueue<Exception> errorsAgain = new ArrayBlockingQueue<>(THREAD_NUMBER);
    ArrayBlockingQueue<Integer> resultsAgain = new ArrayBlockingQueue<>(THREAD_NUMBER);
    List<Future<Void>> futuresAgain = reader.apply(errorsAgain, resultsAgain, null, null);
    waitForReaders(futuresAgain);
    stateCheckAfterReadersFinishAgain.apply(errorsAgain, resultsAgain);
  }

  private void testConcurrentReaderAndClear(
      QuadFunction<ArrayBlockingQueue<Exception>, ArrayBlockingQueue<Integer>,
          CountDownLatch, CountDownLatch, List<Future<Void>>> reader,
      BiFunction<ArrayBlockingQueue<Exception>, ArrayBlockingQueue<Integer>,
          Void> stateCheckAfterReadersFinish,
      BiFunction<ArrayBlockingQueue<Exception>, ArrayBlockingQueue<Integer>,
          Void> stateCheckAfterReadersFinishAgain
  ) throws Exception {
    prepareFiles(FILE_NUMBER);

    CountDownLatch readerRunningLatch = new CountDownLatch(THREAD_NUMBER);
    CountDownLatch writerCompletedLatch = new CountDownLatch(1);
    ArrayBlockingQueue<Exception> errors = new ArrayBlockingQueue<>(THREAD_NUMBER);
    ArrayBlockingQueue<Integer> results = new ArrayBlockingQueue<>(THREAD_NUMBER);
    List<Future<Void>> futures =
        reader.apply(errors, results, readerRunningLatch, writerCompletedLatch);

    // Await for the 20 threads to be iterating in the middle, then trigger the shutdown event
    readerRunningLatch.await();
    mStore.clear();
    writerCompletedLatch.countDown();

    waitForReaders(futures);
    stateCheckAfterReadersFinish.apply(errors, results);

    // Verify that the RocksDB can still serve
    ArrayBlockingQueue<Exception> errorsAgain = new ArrayBlockingQueue<>(THREAD_NUMBER);
    ArrayBlockingQueue<Integer> resultsAgain = new ArrayBlockingQueue<>(THREAD_NUMBER);
    List<Future<Void>> futuresAgain = reader.apply(errorsAgain, resultsAgain, null, null);
    waitForReaders(futuresAgain);
    stateCheckAfterReadersFinishAgain.apply(errorsAgain, resultsAgain);
  }

  private void prepareFiles(int fileCount) throws Exception {
    for (int i = 1; i < fileCount + 1; i++) {
      MutableInodeDirectory dir = MutableInodeDirectory.create(i, 0, "dir" + i,
          CreateDirectoryContext.defaults());
      mStore.addChild(0, dir);
      mStore.writeInode(dir);
    }
  }

  @FunctionalInterface
  interface QuadFunction<A, B, C, D, R> {
    R apply(A a, B b, C c, D d);
  }
}
