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

    // Concurrent put.
    final AtomicInteger isId = new AtomicInteger(0);
    Thread putIs = new Thread(new Runnable() {
      @Override
      public void run() {
        isId.set(streamCache.put(is));
      }
    });
    final AtomicInteger osId = new AtomicInteger(0);
    Thread putOs = new Thread(new Runnable() {
      @Override
      public void run() {
        osId.set(streamCache.put(os));
      }
    });
    putIs.start();
    putOs.start();
    putIs.join();
    putOs.join();
    Assert.assertSame(is, streamCache.getInStream(isId.get()));
    Assert.assertSame(os, streamCache.getOutStream(osId.get()));

    // Concurrent get.
    Thread getIs = new Thread(new Runnable() {
      @Override
      public void run() {
        Assert.assertSame(is, streamCache.getInStream(isId.get()));
      }
    });
    Thread getOs = new Thread(new Runnable() {
      @Override
      public void run() {
        Assert.assertSame(os, streamCache.getOutStream(osId.get()));
      }
    });
    getIs.start();
    getOs.start();
    getIs.join();
    getOs.join();

    // Concurrent get, put, and invalidate.
    final Integer oldIsId = isId.get();
    Thread invalidateIs = new Thread(new Runnable() {
      @Override
      public void run() {
        Assert.assertSame(is, streamCache.invalidate(oldIsId));
      }
    });
    putIs.start();
    invalidateIs.start();
    getOs.start();
    putIs.join();
    invalidateIs.join();
    getOs.join();
    Assert.assertNull(streamCache.getInStream(oldIsId));
    Assert.assertSame(is, streamCache.getInStream(isId.get()));
    Assert.assertSame(os, streamCache.getOutStream(osId.get()));
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
}
