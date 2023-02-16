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

package alluxio.proxy.s3;

import static alluxio.Constants.KB;
import static alluxio.Constants.MB;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;

public class RateLimitInputStreamTest {

  private byte[] mData;

  @Before
  public void init() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(MB);
    int count = 0;
    while (count < MB) {
      byte[] bytes = UUID.randomUUID().toString().getBytes();
      byteArrayOutputStream.write(bytes);
      count += bytes.length;
    }
    mData = Arrays.copyOf(byteArrayOutputStream.toByteArray(), MB);
  }

  @Test
  public void testSingleThreadRead() throws IOException {
    Random random = new Random();
    for (int i = 1; i <= 5; i++) {
      long rate1 = (random.nextInt(4) + 1) * 100 * KB;
      long rate2 = (random.nextInt(4) + 1) * 100 * KB;
      ByteArrayInputStream inputStream = new ByteArrayInputStream(mData);
      RateLimitInputStream rateLimitInputStream = new RateLimitInputStream(inputStream,
          RateLimiter.create(rate1), RateLimiter.create(rate2));
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(MB);
      long start = System.currentTimeMillis();
      IOUtils.copy(rateLimitInputStream, byteArrayOutputStream, KB);
      long end = System.currentTimeMillis();
      long duration = end - start;
      long expectedDuration = MB / Math.min(rate1, rate2) * 1000;
      Assert.assertTrue(duration >= expectedDuration && duration <= expectedDuration + 1000);
      Assert.assertArrayEquals(mData, byteArrayOutputStream.toByteArray());
    }
  }

  private void testMultiThreadRead(long globalRate, long rate, int threadNum) {
    long totalSize = (long) threadNum * mData.length;
    RateLimiter globalRateLimiter = RateLimiter.create(globalRate);
    ExecutorService threadPool = Executors.newFixedThreadPool(threadNum);
    List<FutureTask<byte[]>> tasks = new ArrayList<>();
    for (int i = 1; i <= threadNum; i++) {
      tasks.add(new FutureTask<>(() -> {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(mData);
        RateLimitInputStream rateLimitInputStream = new RateLimitInputStream(inputStream,
            RateLimiter.create(rate), globalRateLimiter);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(MB);
        IOUtils.copy(rateLimitInputStream, byteArrayOutputStream, KB);
        return byteArrayOutputStream.toByteArray();
      }));
    }
    long start = System.currentTimeMillis();
    tasks.forEach(threadPool::submit);
    List<byte[]> results;
    try {
      results = tasks.stream().map(task -> {
        try {
          return task.get();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }).collect(Collectors.toList());
    } finally {
      threadPool.shutdownNow();
    }
    long end = System.currentTimeMillis();
    long duration = end - start;
    long expectedDuration = totalSize / Math.min(globalRate, (long) threadNum * rate) * 1000;
    Assert.assertTrue(duration >= expectedDuration && duration <= expectedDuration + 1000);
    results.forEach(bytes -> Assert.assertArrayEquals(mData, bytes));
  }

  @Test
  public void testMultiThreadReadWithBiggerGlobalRate() {
    testMultiThreadRead(400 * KB, 100 * KB, 3);
  }

  @Test
  public void testMultiThreadReadWithSmallerGlobalRate() {
    testMultiThreadRead(100 * KB, 200 * KB, 3);
  }
}
