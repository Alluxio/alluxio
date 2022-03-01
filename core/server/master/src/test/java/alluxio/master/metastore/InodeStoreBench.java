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

package alluxio.master.metastore;

import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.meta.MutableInodeDirectory;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.metastore.heap.HeapInodeStore;
import alluxio.master.metastore.rocks.RocksInodeStore;
import alluxio.util.CommonUtils;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Microbenchmarks for the inode store.
 */
public class InodeStoreBench {
  private static final int CHECKPOINT_INODES_MILLIONS = 1;
  private static InodeStore sStore;
  private static final AtomicInteger NEXT_INODE_ID = new AtomicInteger(1);

  public static void main(String[] args) throws Exception {
    // Enable logging to stdout.
    Layout layout = new PatternLayout("%d [%t] %-5p %c %x - %m%n");
    Logger.getRootLogger().addAppender(new ConsoleAppender(layout));

    System.out.printf("Running benchmarks for rocks inode store%n");
    sStore = new RocksInodeStore(ServerConfiguration.get(PropertyKey.MASTER_METASTORE_DIR));
    runBenchmarks();

    System.out.printf("%nRunning benchmarks for heap inode store%n");
    sStore = new HeapInodeStore();
    runBenchmarks();
  }

  private static void runBenchmarks() throws Exception {
    writeBenchmark();
    checkpointBenchmark();
  }

  private static void writeBenchmark() throws InterruptedException {
    // warm up
    doForMs(2 * Constants.SECOND_MS, InodeStoreBench::writeInode, new CyclicBarrier(1));

    ExecutorService service = Executors.newCachedThreadPool();
    for (int i = 0; i < 5; i++) {
      sStore.clear();
      int numThreads = 4;
      long timeMs = 3 * Constants.SECOND_MS;
      CyclicBarrier barrier = new CyclicBarrier(numThreads);
      AtomicInteger count = new AtomicInteger(0);
      List<Callable<Void>> threads =
          IntStream.range(0, numThreads).mapToObj(x -> (Callable<Void>) () -> {
            count.addAndGet(doForMs(timeMs, InodeStoreBench::writeInode, barrier));
            return null;
          }).collect(Collectors.toList());
      service.invokeAll(threads);
      System.out.printf("Performed %d operations using %d threads in %dms%n", count.get(),
          numThreads, timeMs);
    }
    service.shutdownNow();
  }

  private static void checkpointBenchmark() throws Exception {
    sStore.clear();
    System.out.printf("Writing %d million inodes ...", CHECKPOINT_INODES_MILLIONS);
    for (int i = 0; i < 1e6 * CHECKPOINT_INODES_MILLIONS; i++) {
      writeInode();
    }
    System.out.println(" done");

    File f = File.createTempFile("checkpoint", "");
    try (OutputStream out = new BufferedOutputStream(new FileOutputStream(f))) {
      long start = System.nanoTime();
      sStore.writeToCheckpoint(out);
      System.out.printf("Wrote %d million inode checkpoint in %dms%n", CHECKPOINT_INODES_MILLIONS,
          (System.nanoTime() - start) / Constants.MS_NANO);
    }
    try (InputStream in = new BufferedInputStream(new FileInputStream(f))) {
      long start = System.nanoTime();
      sStore.restoreFromCheckpoint(new CheckpointInputStream(in));
      System.out.printf("Restored %d million inode checkpoint in %dms%n",
          CHECKPOINT_INODES_MILLIONS, (System.nanoTime() - start) / Constants.MS_NANO);
    }
  }

  private static int doForMs(long timeMs, Runnable action, CyclicBarrier barrier) {
    try {
      barrier.await();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    long start = System.nanoTime();
    long endTime = start + (timeMs * 1_000_000);
    int count = 0;
    while (System.nanoTime() < endTime) {
      action.run();
      count++;
    }
    return count;
  }

  private static void writeInode() {
    int id = NEXT_INODE_ID.getAndIncrement();
    CreateDirectoryContext createContext = CreateDirectoryContext.defaults();
    MutableInodeDirectory dir =
        MutableInodeDirectory.create(id, 0, CommonUtils.randomAlphaNumString(30), createContext);
    sStore.writeInode(dir);
  }
}
