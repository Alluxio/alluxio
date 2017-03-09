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

package alluxio;

import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class StreamCacheTest {
  @Test
  public void operations() throws Exception {
    StreamCache streamCache = new StreamCache(Constants.HOUR_MS);
    FileInStream is = Mockito.mock(FileInStream.class);
    FileOutStream os = Mockito.mock(FileOutStream.class);
    Integer isId = streamCache.put(is);
    Integer osId = streamCache.put(os);
    Assert.assertSame(is, streamCache.getInStream(isId));
    Assert.assertNull(streamCache.getInStream(osId));
    Assert.assertNull(streamCache.getOutStream(isId));
    Assert.assertSame(os, streamCache.getOutStream(osId));
    Assert.assertSame(is, streamCache.invalidate(isId));
    Assert.assertSame(os, streamCache.invalidate(osId));
    Assert.assertNull(streamCache.invalidate(isId));
    Assert.assertNull(streamCache.invalidate(osId));
    Assert.assertNull(streamCache.getInStream(isId));
    Assert.assertNull(streamCache.getOutStream(osId));
    Mockito.verify(is).close();
    Mockito.verify(os).close();
  }

  @Test
  public void concurrentOperations() throws Exception {
    final StreamCache streamCache = new StreamCache(Constants.HOUR_MS);
    final FileInStream is = Mockito.mock(FileInStream.class);
    final FileOutStream os = Mockito.mock(FileOutStream.class);
    final int numThreads = 100;

    // Concurrent put.
    final AtomicIntegerArray isIDs = new AtomicIntegerArray(numThreads);
    final AtomicInteger isIDsIndex = new AtomicInteger();
    final AtomicIntegerArray osIDs = new AtomicIntegerArray(numThreads);
    final AtomicInteger osIDsIndex = new AtomicInteger();
    Runnable put = new Runnable() {
      @Override
      public void run() {
        int id = streamCache.put(is);
        int index = isIDsIndex.getAndIncrement();
        isIDs.set(index, id);
        id = streamCache.put(os);
        index = osIDsIndex.getAndIncrement();
        osIDs.set(index, id);
      }
    };
    Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      Thread thread = new Thread(put);
      threads[i] = thread;
      thread.start();
    }
    for (Thread thread : threads) {
      thread.join();
    }
    Assert.assertEquals(numThreads, isIDsIndex.get());
    Assert.assertEquals(numThreads, osIDsIndex.get());
    for (int i = 0; i < numThreads; i++) {
      Assert.assertSame(is, streamCache.getInStream(isIDs.get(i)));
      Assert.assertSame(os, streamCache.getOutStream(osIDs.get(i)));
    }
    Assert.assertEquals(2 * numThreads, streamCache.size());

    // Concurrent get.
    class Get implements Runnable {
      private int mThreadIndex;

      Get(int threadIndex) {
        mThreadIndex = threadIndex;
      }

      @Override
      public void run() {
        Assert.assertSame(is, streamCache.getInStream(isIDs.get(mThreadIndex)));
        Assert.assertSame(os, streamCache.getOutStream(osIDs.get(mThreadIndex)));
      }
    }

    for (int i = 0; i < numThreads; i++) {
      threads[i] = new Thread(new Get(i));
    }
    for (Thread thread : threads) {
      thread.start();
    }
    for (Thread thread : threads) {
      thread.join();
    }
    for (int i = 0; i < numThreads; i++) {
      Assert.assertSame(is, streamCache.getInStream(isIDs.get(i)));
      Assert.assertSame(os, streamCache.getOutStream(osIDs.get(i)));
    }
    Assert.assertEquals(2 * numThreads, streamCache.size());

    // Concurrent get, put, and invalidate.
    class GetPutInvalidate implements Runnable {
      private int mThreadIndex;

      GetPutInvalidate(int threadIndex) {
        mThreadIndex = threadIndex;
      }

      @Override
      public void run() {
        Assert.assertSame(is, streamCache.getInStream(isIDs.get(mThreadIndex)));
        Assert.assertSame(os, streamCache.getOutStream(osIDs.get(mThreadIndex)));
        int isId = streamCache.put(is);
        int osId = streamCache.put(os);
        Assert.assertSame(is, streamCache.invalidate(isIDs.get(mThreadIndex)));
        Assert.assertSame(os, streamCache.invalidate(osIDs.get(mThreadIndex)));
        Assert.assertSame(is, streamCache.invalidate(isId));
        Assert.assertSame(os, streamCache.invalidate(osId));
      }
    }

    for (int i = 0; i < numThreads; i++) {
      threads[i] = new Thread(new GetPutInvalidate(i));
    }
    for (Thread thread : threads) {
      thread.start();
    }
    for (Thread thread : threads) {
      thread.join();
    }
    for (int i = 0; i < numThreads; i++) {
      Assert.assertNull(streamCache.getInStream(isIDs.get(i)));
      Assert.assertNull(streamCache.getOutStream(osIDs.get(i)));
    }
    Assert.assertEquals(0, streamCache.size());

    // Concurrent put, get, and invalidate.
    Runnable putGetInvalidate = new Runnable() {
      @Override
      public void run() {
        int isId = streamCache.put(is);
        int osId = streamCache.put(os);
        Assert.assertSame(is, streamCache.getInStream(isId));
        Assert.assertSame(os, streamCache.getOutStream(osId));
        Assert.assertSame(is, streamCache.invalidate(isId));
        Assert.assertSame(os, streamCache.invalidate(osId));
      }
    };
    for (int i = 0; i < numThreads; i++) {
      threads[i] = new Thread(putGetInvalidate);
    }
    for (Thread thread : threads) {
      thread.start();
    }
    for (Thread thread : threads) {
      thread.join();
    }
    Assert.assertEquals(0, streamCache.size());
  }

  @Test
  public void expiration() throws Exception {
    StreamCache streamCache = new StreamCache(0);
    FileInStream is = Mockito.mock(FileInStream.class);
    FileOutStream os = Mockito.mock(FileOutStream.class);
    streamCache.put(is);
    streamCache.put(os);
    Mockito.verify(is).close();
    Mockito.verify(os).close();
  }

  @Test
  public void size() throws Exception {
    StreamCache streamCache = new StreamCache(Constants.HOUR_MS);
    FileInStream is = Mockito.mock(FileInStream.class);
    FileOutStream os = Mockito.mock(FileOutStream.class);
    Assert.assertEquals(0, streamCache.size());
    int isId = streamCache.put(is);
    Assert.assertEquals(1, streamCache.size());
    int osId = streamCache.put(os);
    Assert.assertEquals(2, streamCache.size());
    streamCache.invalidate(isId);
    Assert.assertEquals(1, streamCache.size());
    streamCache.invalidate(osId);
    Assert.assertEquals(0, streamCache.size());
  }
}
