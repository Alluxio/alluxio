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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public final class StreamCacheTest {
  @Test
  public void operations() throws Exception {
    StreamCache streamCache = new StreamCache(Constants.HOUR_MS);
    FileInStream is = mock(FileInStream.class);
    FileOutStream os = mock(FileOutStream.class);
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
    verify(is).close();
    verify(os).close();
  }

  @Test
  public void concurrentOperations() throws Exception {
    final StreamCache streamCache = new StreamCache(Constants.HOUR_MS);

    class CacheTest<T> implements Runnable {
      private T mStream;

      CacheTest(T stream) {
        mStream = stream;
      }

      @Override
      public void run() {
        int cacheID = 0;
        if (mStream instanceof FileInStream) {
          cacheID = streamCache.put((FileInStream) mStream);
          Assert.assertSame(mStream, streamCache.getInStream(cacheID));
        }
        if (mStream instanceof FileOutStream) {
          cacheID = streamCache.put((FileOutStream) mStream);
          Assert.assertSame(mStream, streamCache.getOutStream(cacheID));
        }
        Assert.assertSame(mStream, streamCache.invalidate(cacheID));
      }
    }

    final int numOps = 100;
    final List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < numOps; i++) {
      threads.add(new Thread(new CacheTest<>(mock(FileInStream.class))));
      threads.add(new Thread(new CacheTest<>(mock(FileOutStream.class))));
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
    FileInStream is = mock(FileInStream.class);
    FileOutStream os = mock(FileOutStream.class);
    streamCache.put(is);
    streamCache.put(os);
    verify(is).close();
    verify(os).close();
  }

  @Test
  public void size() throws Exception {
    StreamCache streamCache = new StreamCache(Constants.HOUR_MS);
    FileInStream is = mock(FileInStream.class);
    FileOutStream os = mock(FileOutStream.class);
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
