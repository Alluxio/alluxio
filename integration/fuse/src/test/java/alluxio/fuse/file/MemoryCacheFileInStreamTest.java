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

package alluxio.fuse.file;

import alluxio.Constants;
import alluxio.client.file.MockFileInStream;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.wire.FileInfo;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

public class MemoryCacheFileInStreamTest {

  private final Random mRandom = new Random();

  @Before
  public void setConfig() {
    Configuration.set(PropertyKey.FUSE_MEMORY_CACHE_PAGE_COUNT, 4);
    Configuration.set(PropertyKey.FUSE_MEMORY_CACHE_PAGE_SIZE, "4KB");
    Configuration.set(PropertyKey.FUSE_MEMORY_CACHE_CONCURRENCY_LEVEL, 4);
  }

  @Test
  public void testSingleThread() throws IOException, AlluxioException {
    // test empty data
    testRead(0, 1);

    // dataSize < buffer
    for (int i = 0; i < 1000; i++) {
      testRead(mRandom.nextInt(1024), 1024 + mRandom.nextInt(1024));
    }

    // dataSize > buffer && dataSize < 4KB(page size)
    for (int i = 0; i < 1000; i++) {
      testRead(Constants.KB + mRandom.nextInt(3 * Constants.KB), 128 + mRandom.nextInt(128));
    }

    // dataSize > 4KB && dataSize < 16KB(cache bytes)
    for (int i = 0; i < 1000; i++) {
      testRead(4 * Constants.KB + mRandom.nextInt(12 * Constants.KB), 128 + mRandom.nextInt(128));
    }

    // dataSize > 16KB(cache bytes)
    for (int i = 0; i < 1000; i++) {
      testRead(16 * Constants.KB + mRandom.nextInt(Constants.KB), 128 + mRandom.nextInt(128));
    }
  }

  @Test
  public void testMultipleThreadSameData() throws ExecutionException, InterruptedException {
    ExecutorService pool = Executors.newFixedThreadPool(8 + mRandom.nextInt(8));
    try {
      byte[] bytes = new byte[64 * Constants.KB + mRandom.nextInt(Constants.KB)];
      mRandom.nextBytes(bytes);
      List<FutureTask<?>> tasks = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {
        FutureTask<Object> task = new FutureTask<>(() -> {
          testRead(bytes, 128 + mRandom.nextInt(128));
          return null;
        });
        tasks.add(task);
        pool.submit(task);
      }
      for (FutureTask<?> task : tasks) {
        task.get();
      }
    } finally {
      pool.shutdown();
    }
  }

  @Test
  public void testMultipleThreadDifferentData() throws ExecutionException, InterruptedException {
    ExecutorService pool = Executors.newFixedThreadPool(8 + mRandom.nextInt(8));
    try {
      byte[] bytes1 = new byte[64 * Constants.KB + mRandom.nextInt(Constants.KB)];
      byte[] bytes2 = new byte[64 * Constants.KB + mRandom.nextInt(Constants.KB)];
      mRandom.nextBytes(bytes1);
      mRandom.nextBytes(bytes2);
      List<FutureTask<?>> tasks = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {
        FutureTask<Object> task;
        if (mRandom.nextBoolean()) {
          task = new FutureTask<>(() -> {
            testRead(bytes1, 128 + mRandom.nextInt(4096));
            return null;
          });
        } else {
          task = new FutureTask<>(() -> {
            testRead(bytes2, 128 + mRandom.nextInt(4096));
            return null;
          });
        }
        tasks.add(task);
        pool.submit(task);
      }
      for (FutureTask<?> task : tasks) {
        task.get();
      }
    } finally {
      pool.shutdown();
    }
  }

  private void testRead(byte[] bytes, int bufferSize)
      throws IOException, AlluxioException {
    try (MemoryCacheFileInStream memoryCacheFileInStream = createRandomMemoryCacheFileInStream(
        bytes);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      IOUtils.copy(memoryCacheFileInStream, outputStream, bufferSize);
      Assert.assertArrayEquals(bytes, outputStream.toByteArray());
    }
  }

  private void testRead(int dataSize, int bufferSize)
      throws IOException, AlluxioException {
    byte[] bytes = new byte[dataSize];
    mRandom.nextBytes(bytes);
    testRead(bytes, bufferSize);
  }

  private MemoryCacheFileInStream createRandomMemoryCacheFileInStream(byte[] bytes)
      throws IOException, AlluxioException {
    MockFileInStream fileInStream = new MockFileInStream(bytes);
    FileInfo fileInfo = new FileInfo();
    fileInfo.setPath(UUID.randomUUID().toString());
    fileInfo.setLastModificationTimeMs(Math.abs(mRandom.nextLong()));
    fileInfo.setLength(bytes.length);
    return new MemoryCacheFileInStream(new URIStatus(fileInfo), fileInStream);
  }
}
