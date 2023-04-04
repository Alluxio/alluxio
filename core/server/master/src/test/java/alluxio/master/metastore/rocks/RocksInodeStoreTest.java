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

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.file.meta.MutableInodeDirectory;
import alluxio.master.journal.JournalUtils;
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
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

public class RocksInodeStoreTest {
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  public RocksInodeStore mStore;

  @Before
  public void setUp() throws Exception {
    // Be explicit in this test
    Configuration.set(PropertyKey.TEST_MODE, true);
    // Wait for a shorter period of time in test
    Configuration.set(PropertyKey.MASTER_METASTORE_ROCKS_EXCLUSIVE_LOCK_TIMEOUT, "1s");
    mStore = new RocksInodeStore(mFolder.newFolder().getAbsolutePath());
  }

  @After
  public void tearDown() throws Exception {
    mStore.close();
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

    mStore.writeInode(MutableInodeDirectory.create(1, 0, "dir", CreateDirectoryContext.defaults()));
    assertEquals("dir", mStore.get(1).get().getName());
    assertThat(mStore.toStringEntries(), containsString("name=dir"));
  }

  @Test
  public void refCount() {
    for (int i = 1; i < 20; i++) {
      MutableInodeDirectory dir = MutableInodeDirectory.create(i, 0, "dir" + i, CreateDirectoryContext.defaults());
      mStore.addChild(0, dir);
    }

    try (CloseableIterator<Long> iter = mStore.getChildIds(0L)) {
      int i = 0;
      while (iter.hasNext()) {
        System.out.println("RocksStore has refCount=" + mStore.mRocksStore.mRefCount.sum());

        if (i == 10) {
          mStore.mRocksStore.mStopServing.set(true);
          System.out.println("Marked RocksDB for closing");
        }

        long id = iter.next();
        i++;
      }
    } catch (Exception e) {
      System.out.println("Caught ex from iterator " + e.getMessage());
    } finally {
      System.out.println("End - RocksStore has refCount=" + mStore.mRocksStore.mRefCount.sum());
    }
  }




  @Test
  public void concurrentListAndClose() throws Exception {
    int fileNumber = 400;
    int threadCount = 20;
    prepareFiles(fileNumber);

    ExecutorService threadpool = Executors.newFixedThreadPool(threadCount, ThreadFactoryUtils.build("test-executor-%d", true));

    CountDownLatch latch = new CountDownLatch(20);
    List<Future> futures = new ArrayList<>();
    ArrayBlockingQueue<Exception> queue = new ArrayBlockingQueue<>(threadCount);
    ArrayBlockingQueue<Integer> results = new ArrayBlockingQueue<>(threadCount);
    for (int k = 0; k < threadCount; k++) {
      futures.add(threadpool.submit(() -> {
        int listedCount = 0;
        try (CloseableIterator<Long> iter = mStore.getChildIds(0L)) {
          while (iter.hasNext()) {
            if (listedCount == 10) {
              latch.countDown();
            }
            iter.next();
            listedCount++;
          }
        } catch (Exception e) {
          queue.add(e);
        } finally {
          results.add(listedCount);
          System.out.println("End - RocksStore has refCount=" + mStore.mRocksStore.mRefCount.sum());
        }
      }));
    }

    // Await for the 20 threads to be iterating in the middle, then trigger the shutdown event
    latch.await();
    System.out.println("All 20 threads are running, shut down now");
    mStore.close();

    futures.stream().forEach(f -> {
      try {
        f.get();
      } catch (Exception e) {
        fail("Met uncaught exception from iteration");
      }
    });
    System.out.println("All threads finished");
    // Reaching here means close() was successfully, which implies ref count reached zero
    // By chance, some iterations may complete so not all iterations will abort
    assertTrue(queue.size() <= threadCount);
    long completed = results.stream().filter(n -> n == fileNumber).count();
    // List attempts either completed or aborted
    assertEquals(completed + queue.size(), threadCount);
  }

  @Test
  public void concurrentListAndClear() throws Exception {
    int fileNumber = 400;
    int threadCount = 20;
    prepareFiles(fileNumber);

    ExecutorService threadpool = Executors.newFixedThreadPool(threadCount, ThreadFactoryUtils.build("test-executor-%d", true));

    CountDownLatch latch = new CountDownLatch(20);
    List<Future> futures = new ArrayList<>();
    ArrayBlockingQueue<Exception> queue = new ArrayBlockingQueue<>(threadCount);
    ArrayBlockingQueue<Integer> results = new ArrayBlockingQueue<>(threadCount);
    for (int k = 0; k < threadCount; k++) {
      futures.add(threadpool.submit(() -> {
        int listedCount = 0;
        try (CloseableIterator<Long> iter = mStore.getChildIds(0L)) {
          while (iter.hasNext()) {
            if (listedCount == 10) {
              latch.countDown();
            }
            iter.next();
            listedCount++;
          }
        } catch (Exception e) {
          queue.add(e);
        } finally {
          results.add(listedCount);
          System.out.println("End - RocksStore has refCount=" + mStore.mRocksStore.mRefCount.sum());
        }
      }));
    }

    // Await for the 20 threads to be iterating in the middle, then trigger the shutdown event
    latch.await();
    System.out.println("All 20 threads are running, shut down now");
    mStore.clear();

    futures.stream().forEach(f -> {
      try {
        f.get();
      } catch (Exception e) {
        fail("Met uncaught exception from iteration");
      }
    });
    System.out.println("All threads finished");
    // Reaching here means close() was successfully, which implies ref count reached zero
    // By chance, some iterations may complete so not all iterations will abort
    assertTrue(queue.size() <= threadCount);
    long completed = results.stream().filter(n -> n == fileNumber).count();
    // List attempts either completed or aborted
    assertEquals(completed + queue.size(), threadCount);

    // Verify that the RocksDB can still serve
    prepareFiles(fileNumber);


    List<Future> futuresAgain = new ArrayList<>();
    ArrayBlockingQueue<Exception> queueAgain = new ArrayBlockingQueue<>(threadCount);
    ArrayBlockingQueue<Integer> resultsAgain = new ArrayBlockingQueue<>(threadCount);
    for (int k = 0; k < threadCount; k++) {
      futuresAgain.add(threadpool.submit(() -> {
        int listedCount = 0;
        try (CloseableIterator<Long> iter = mStore.getChildIds(0L)) {
          while (iter.hasNext()) {
            iter.next();
            listedCount++;
          }
        } catch (Exception e) {
          queueAgain.add(e);
        } finally {
          resultsAgain.add(listedCount);
          System.out.println("End - RocksStore has refCount=" + mStore.mRocksStore.mRefCount.sum());
        }
      }));
    }
    futuresAgain.stream().forEach(f -> {
      try {
        f.get();
      } catch (Exception e) {
        fail("Met uncaught exception from iteration");
      }
    });
    System.out.println("All threads finished on the new RocksDB");

    assertEquals(0, queueAgain.size());
    long completedAgain = resultsAgain.stream().filter(n -> n == fileNumber).count();
    assertEquals(20, completedAgain);
  }


  @Test
  // This only happens on primary and when checkpoint is specified to happen on primary
  public void concurrentListAndCheckpoint() throws Exception {
    int fileNumber = 400;
    int threadCount = 20;
    prepareFiles(fileNumber);

    ExecutorService threadpool = Executors.newFixedThreadPool(threadCount, ThreadFactoryUtils.build("test-executor-%d", true));

    CountDownLatch latch = new CountDownLatch(20);
    List<Future> futures = new ArrayList<>();
    ArrayBlockingQueue<Exception> queue = new ArrayBlockingQueue<>(threadCount);
    ArrayBlockingQueue<Integer> results = new ArrayBlockingQueue<>(threadCount);
    for (int k = 0; k < threadCount; k++) {
      futures.add(threadpool.submit(() -> {
        int listedCount = 0;
        try (CloseableIterator<Long> iter = mStore.getChildIds(0L)) {
          while (iter.hasNext()) {
            if (listedCount == 10) {
              latch.countDown();
            }
            iter.next();
            listedCount++;
          }
        } catch (Exception e) {
          queue.add(e);
        } finally {
          results.add(listedCount);
          System.out.println("End - RocksStore has refCount=" + mStore.mRocksStore.mRefCount.sum());
        }
      }));
    }

    // Await for the 20 threads to be iterating in the middle, then trigger the shutdown event
    latch.await();
    System.out.println("All 20 threads are running, shut down now");
    File checkpointFile = mFolder.newFile("checkpoint-file");
    try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(checkpointFile))) {
      mStore.writeToCheckpoint(out);
    }
    System.out.println("Write to checkpoint finished, now the RocksDB should be back");
    System.out.println("The checkpoint file is " + Files.size(checkpointFile.toPath()));

    futures.stream().forEach(f -> {
      try {
        f.get();
      } catch (Exception e) {
        fail("Met uncaught exception from iteration");
      }
    });
    System.out.println("All threads finished");
    // Reaching here means close() was successfully, which implies ref count reached zero
    // By chance, some iterations may complete so not all iterations will abort
    assertTrue(queue.size() <= threadCount);
    long completed = results.stream().filter(n -> n == fileNumber).count();
    // List attempts either completed or aborted
    assertEquals(completed + queue.size(), threadCount);

    System.out.println("Submit new operations to RocksDB");
    List<Future> futuresAgain = new ArrayList<>();
    ArrayBlockingQueue<Exception> queueAgain = new ArrayBlockingQueue<>(threadCount);
    ArrayBlockingQueue<Integer> resultsAgain = new ArrayBlockingQueue<>(threadCount);
    for (int k = 0; k < threadCount; k++) {
      futuresAgain.add(threadpool.submit(() -> {
        int listedCount = 0;
        try (CloseableIterator<Long> iter = mStore.getChildIds(0L)) {
          while (iter.hasNext()) {
            iter.next();
            listedCount++;
          }
        } catch (Exception e) {
          queueAgain.add(e);
        } finally {
          resultsAgain.add(listedCount);
          System.out.println("End - RocksStore has refCount=" + mStore.mRocksStore.mRefCount.sum());
        }
      }));
    }
    futuresAgain.stream().forEach(f -> {
      try {
        f.get();
      } catch (Exception e) {
        fail("Met uncaught exception from iteration");
      }
    });
    System.out.println("All threads finished on the new RocksDB");

    assertEquals(0, queueAgain.size());
    long completedAgain = resultsAgain.stream().filter(n -> n == fileNumber).count();
    assertEquals(20, completedAgain);
  }

  @Test
  public void concurrentListAndRestore() throws Exception {
    int fileNumber = 400;
    int threadCount = 20;
    prepareFiles(fileNumber);
    // Prepare a checkpoint file
    File checkpointFile = File.createTempFile("checkpoint-for-recovery", "");
    try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(checkpointFile))) {
      mStore.writeToCheckpoint(out);
    }
    System.out.println("Prepared checkpoint file with size " + Files.size(checkpointFile.toPath()));


    ExecutorService threadpool = Executors.newFixedThreadPool(threadCount, ThreadFactoryUtils.build("test-executor-%d", true));

    CountDownLatch latch = new CountDownLatch(20);
    List<Future> futures = new ArrayList<>();
    ArrayBlockingQueue<Exception> queue = new ArrayBlockingQueue<>(threadCount);
    ArrayBlockingQueue<Integer> results = new ArrayBlockingQueue<>(threadCount);
    for (int k = 0; k < threadCount; k++) {
      futures.add(threadpool.submit(() -> {
        int listedCount = 0;
        try (CloseableIterator<Long> iter = mStore.getChildIds(0L)) {
          while (iter.hasNext()) {
            if (listedCount == 10) {
              latch.countDown();
            }
            iter.next();
            listedCount++;
          }
        } catch (Exception e) {
          queue.add(e);
        } finally {
          results.add(listedCount);
          System.out.println("End - RocksStore has refCount=" + mStore.mRocksStore.mRefCount.sum());
        }
      }));
    }

    // Await for the 20 threads to be iterating in the middle, then trigger the shutdown event
    latch.await();
    System.out.println("All 20 threads are running, shut down now");
    try (CheckpointInputStream in = new CheckpointInputStream((new DataInputStream(new FileInputStream(checkpointFile))))) {
      mStore.restoreFromCheckpoint(in);
    }
    System.out.println("Restored from checkpoint");

    futures.stream().forEach(f -> {
      try {
        f.get();
      } catch (Exception e) {
        fail("Met uncaught exception from iteration");
      }
    });
    System.out.println("All threads finished");
    // Reaching here means close() was successfully, which implies ref count reached zero
    // By chance, some iterations may complete so not all iterations will abort
    assertTrue(queue.size() <= threadCount);
    long completed = results.stream().filter(n -> n == fileNumber).count();
    // List attempts either completed or aborted
    assertEquals(completed + queue.size(), threadCount);

    // Verify that the RocksDB can still serve
    System.out.println("Submit new operations to the RocksDB");
    List<Future> futuresAgain = new ArrayList<>();
    ArrayBlockingQueue<Exception> queueAgain = new ArrayBlockingQueue<>(threadCount);
    ArrayBlockingQueue<Integer> resultsAgain = new ArrayBlockingQueue<>(threadCount);
    for (int k = 0; k < threadCount; k++) {
      futuresAgain.add(threadpool.submit(() -> {
        int listedCount = 0;
        try (CloseableIterator<Long> iter = mStore.getChildIds(0L)) {
          while (iter.hasNext()) {
            iter.next();
            listedCount++;
          }
        } catch (Exception e) {
          queueAgain.add(e);
        } finally {
          resultsAgain.add(listedCount);
          System.out.println("End - RocksStore has refCount=" + mStore.mRocksStore.mRefCount.sum());
        }
      }));
    }
    futuresAgain.stream().forEach(f -> {
      try {
        f.get();
      } catch (Exception e) {
        fail("Met uncaught exception from iteration");
      }
    });
    System.out.println("All threads finished on the new RocksDB");

    assertEquals(0, queueAgain.size());
    long completedAgain = resultsAgain.stream().filter(n -> n == fileNumber).count();
    assertEquals(20, completedAgain);
  }


  @Test
  public void concurrentGetAndClose() throws Exception {
    int fileNumber = 400;
    int threadCount = 20;
    prepareFiles(fileNumber);

    // TODO(jiacheng): close resources properly
    ExecutorService threadpool = Executors.newFixedThreadPool(threadCount, ThreadFactoryUtils.build("test-executor-%d", true));

    CountDownLatch latch = new CountDownLatch(20);
    List<Future> futures = new ArrayList<>();
    ArrayBlockingQueue<Exception> queue = new ArrayBlockingQueue<>(threadCount);
    ArrayBlockingQueue<Integer> results = new ArrayBlockingQueue<>(threadCount);

    for (int k = 0; k < threadCount; k++) {
      final int iterNum = k;
      futures.add(threadpool.submit(() -> {
        System.out.println("Running in thread");
        int finishedCount = 0;
        try {
          for (int x = 0; x < 20; x++) {
            long targetInodeId = iterNum * 20 + x;
            System.out.println("target id=" + targetInodeId + " and x=" + x);
            Optional<MutableInode<?>> dir = mStore.getMutable(targetInodeId, ReadOption.defaults());
            System.out.println(dir);
//            assertTrue(dir.isPresent());
            finishedCount++;
            if (x == 10) {
              System.out.println("Count down to latch");
              latch.countDown();
            }
          }
          System.out.println("Loop ended");
        } catch (Exception e) {
          e.printStackTrace();
          System.out.println("Exception " + e.getMessage());
          queue.add(e);
        } finally {
          results.add(finishedCount);
          System.out.println("End - RocksStore has refCount=" + mStore.mRocksStore.mRefCount.sum());
        }
      }));
      System.out.println("Submitted " + k);
    }


    // Await for the 20 threads to be iterating in the middle, then trigger the shutdown event
    System.out.println("All 20 futures submitted, waiting to stop");
    latch.await();
    System.out.println("All 20 threads are running, shut down now");
    mStore.close();

    futures.stream().forEach(f -> {
      try {
        f.get();
      } catch (Exception e) {
        fail("Met uncaught exception from iteration");
      }
    });
    System.out.println("All threads finished");
    // Reaching here means close() was successfully, which implies ref count reached zero
    // By chance, some iterations may complete so not all iterations will abort
    assertTrue(queue.size() <= threadCount);
    long completed = results.stream().filter(n -> n == 20).count();
    // List attempts either completed or aborted
    assertEquals(completed + queue.size(), threadCount);
  }

  @Test
  public void concurrentAddAndClose() throws Exception {
    int fileNumber = 400;
    int threadCount = 20;

    ExecutorService threadpool = Executors.newFixedThreadPool(threadCount, ThreadFactoryUtils.build("test-executor-%d", true));

    CountDownLatch latch = new CountDownLatch(20);
    List<Future> futures = new ArrayList<>();
    ArrayBlockingQueue<Exception> queue = new ArrayBlockingQueue<>(threadCount);
    ArrayBlockingQueue<Integer> results = new ArrayBlockingQueue<>(threadCount);


    for (int k = 0; k < threadCount; k++) {
      final int iterNum = k;
      futures.add(threadpool.submit(() -> {
        int finishedCount = 0;
        try {
          for (int x = 0; x < 20; x++) {
            long targetInodeId = iterNum * 20 + x;
            MutableInodeDirectory dir = MutableInodeDirectory.create(targetInodeId, 0, "dir" + targetInodeId, CreateDirectoryContext.defaults());
            mStore.addChild(0L, dir);
            if (x == 10) {
              latch.countDown();
            }
            finishedCount++;
          }
        } catch (Exception e) {
          queue.add(e);
        } finally {
          results.add(finishedCount);
          System.out.println("End - RocksStore has refCount=" + mStore.mRocksStore.mRefCount.sum());
        }
      }));
    }

    // Await for the 20 threads to be iterating in the middle, then trigger the shutdown event
    latch.await();
    System.out.println("All 20 threads are running, shut down now");
    mStore.close();

    futures.stream().forEach(f -> {
      try {
        f.get();
      } catch (Exception e) {
        fail("Met uncaught exception from iteration");
      }
    });
    System.out.println("All threads finished");
    // Reaching here means close() was successfully, which implies ref count reached zero
    // By chance, some iterations may complete so not all iterations will abort
    assertTrue(queue.size() <= threadCount);
    long completed = results.stream().filter(n -> n == 20).count();
    // List attempts either completed or aborted
    assertEquals(completed + queue.size(), threadCount);
  }


  private void prepareFiles(int fileCount) throws Exception {
    for (int i = 1; i < fileCount + 1; i++) {
      MutableInodeDirectory dir = MutableInodeDirectory.create(i, 0, "dir" + i, CreateDirectoryContext.defaults());
      mStore.addChild(0, dir);
    }
  }




  // TODO(jiacheng): ref count closing Iterator from abort


  // TODO(jiacheng): ref count normally

  // TODO(jiacheng): ref count closing iterator vs listing

  // TODO(jiacheng): concurrent ops and close
}
