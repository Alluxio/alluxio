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
    Configuration.set(PropertyKey.FUSE_MEMORY_CACHE_PAGE_COUNT, 16);
  }

  @Test
  public void testSingleThread() throws IOException, AlluxioException {
    // test empty data
    testRead(0, 1);

    // dataSize < buffer
    for (int i = 0; i < 100; i++) {
      testRead(1 + mRandom.nextInt(1024), 1024 + mRandom.nextInt(4096));
    }

    // dataSize > buffer && dataSize < 4MB(default page size)
    for (int i = 0; i < 100; i++) {
      testRead(Constants.MB + mRandom.nextInt(3 * Constants.MB), 128 + mRandom.nextInt(4096));
    }

    // dataSize > buffer && dataSize < 4MB
    for (int i = 0; i < 100; i++) {
      testRead(Constants.MB + mRandom.nextInt(3 * Constants.MB), 1024 + mRandom.nextInt(4096));
    }

    // dataSize > 4M && dataSize < 64MB(cache bytes)
    for (int i = 0; i < 100; i++) {
      testRead(4 * Constants.MB + mRandom.nextInt(16 * Constants.MB), 2048 + mRandom.nextInt(4096));
    }

    // dataSize > 64MB(cache bytes)
    for (int i = 0; i < 100; i++) {
      testRead(64 * Constants.MB + mRandom.nextInt(Constants.MB), 2048 + mRandom.nextInt(4096));
    }
  }

  @Test
  public void testMultipleThreadSameData() throws ExecutionException, InterruptedException {
    byte[] bytes = new byte[64 * Constants.MB + mRandom.nextInt(Constants.MB)];
    mRandom.nextBytes(bytes);
    ExecutorService pool = Executors.newFixedThreadPool(4);
    List<FutureTask<?>> tasks = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      FutureTask<Object> task = new FutureTask<>(() -> {
        testRead(bytes, 128 + mRandom.nextInt(4096));
        return null;
      });
      tasks.add(task);
      pool.submit(task);
    }
    for (FutureTask<?> task : tasks) {
      task.get();
    }
    pool.shutdown();
  }

  @Test
  public void testMultipleThreadDifferentData() throws ExecutionException, InterruptedException {
    byte[] bytes1 = new byte[64 * Constants.MB + mRandom.nextInt(Constants.MB)];
    byte[] bytes2 = new byte[64 * Constants.MB + mRandom.nextInt(Constants.MB)];
    mRandom.nextBytes(bytes1);
    mRandom.nextBytes(bytes2);
    ExecutorService pool = Executors.newFixedThreadPool(4);
    List<FutureTask<?>> tasks = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
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
    pool.shutdown();
  }

  private void testRead(byte[] bytes, int bufferSize)
      throws IOException, AlluxioException {
    try (MemoryCacheFileInStream memoryCacheFileInStream = createRandomMemoryCacheFileInStream(
        bytes);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      IOUtils.copy(memoryCacheFileInStream, outputStream, bufferSize);
      Assert.assertArrayEquals(bytes, outputStream.toByteArray());
      MemoryCacheFileInStream.invalidateAllCache();
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
