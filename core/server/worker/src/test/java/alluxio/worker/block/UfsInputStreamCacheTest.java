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

package alluxio.worker.block;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.ConfigurationRule;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.test.util.ConcurrencyUtils;
import alluxio.underfs.SeekableUnderFileInputStream;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.OpenOptions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.Invocation;

import java.io.Closeable;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public final class UfsInputStreamCacheTest {
  private static final String FILE_NAME = "/test";
  private static final long FILE_ID = 1;

  private UnderFileSystem mUfs;
  private SeekableUnderFileInputStream[] mSeekableInStreams;
  private UfsInputStreamCache mManager;
  private int mNumOfInputStreams = 20;

  @Before
  public void before() throws Exception {
    mSeekableInStreams = new SeekableUnderFileInputStream[mNumOfInputStreams];
    mUfs = mock(UnderFileSystem.class);
    when(mUfs.isSeekable()).thenReturn(true);
    for (int i = 0; i < mNumOfInputStreams; i++) {
      SeekableUnderFileInputStream instream = mock(SeekableUnderFileInputStream.class);
      mSeekableInStreams[i] = instream;
    }
    when(mUfs.openExistingFile(eq(FILE_NAME), any(OpenOptions.class))).thenReturn(
        mSeekableInStreams[0], Arrays.copyOfRange(mSeekableInStreams, 1, mNumOfInputStreams));
    mManager = new UfsInputStreamCache();
  }

  @Test
  public void notSeekable() throws Exception {
    when(mUfs.isSeekable()).thenReturn(false);

    SeekableUnderFileInputStream mockedStream = mock(SeekableUnderFileInputStream.class);
    when(mUfs.openExistingFile(eq(FILE_NAME), any(OpenOptions.class)))
        .thenReturn(mockedStream).thenThrow(new IllegalStateException("Should be called once"));

    // acquire a stream
    InputStream instream1 =
        mManager.acquire(mUfs, FILE_NAME, FILE_ID, OpenOptions.defaults().setOffset(2));
    // release
    mManager.release(instream1);

    // ensure the second time the released instream is the same one but repositioned
    verify(mockedStream).close();
  }

  @Test
  public void acquireAndRelease() throws Exception {
    SeekableUnderFileInputStream mockedStream = mock(SeekableUnderFileInputStream.class);
    when(mUfs.openExistingFile(eq(FILE_NAME), any(OpenOptions.class)))
        .thenReturn(mockedStream).thenThrow(new IllegalStateException("Should be called once"));

    // acquire a stream
    InputStream instream1 =
        mManager.acquire(mUfs, FILE_NAME, FILE_ID, OpenOptions.defaults().setOffset(2));
    // release
    mManager.release(instream1);
    // acquire a stream again
    InputStream instream2 =
        mManager.acquire(mUfs, FILE_NAME, FILE_ID, OpenOptions.defaults().setOffset(4));

    Assert.assertEquals(instream1, instream2);
    // ensure the second time the released instream is the same one but repositioned
    verify(mockedStream).seek(4);
  }

  @Test
  public void multipleCheckIn() throws Exception {
    mManager.acquire(mUfs, FILE_NAME, FILE_ID, OpenOptions.defaults().setOffset(2));
    mManager.acquire(mUfs, FILE_NAME, FILE_ID, OpenOptions.defaults().setOffset(4));
    mManager.acquire(mUfs, FILE_NAME, FILE_ID, OpenOptions.defaults().setOffset(6));
    // 3 different input streams are acquired
    verify(mUfs, times(3)).openExistingFile(eq(FILE_NAME),
        any(OpenOptions.class));
  }

  @Test
  public void expire() throws Exception {
    try (Closeable r = new ConfigurationRule(new HashMap<PropertyKey, String>() {
      {
        put(PropertyKey.WORKER_UFS_INSTREAM_CACHE_EXPIRARTION_TIME, "2");
      }
    }, ServerConfiguration.global()).toResource()) {
      mManager = new UfsInputStreamCache();
      // check out a stream
      InputStream instream =
          mManager.acquire(mUfs, FILE_NAME, FILE_ID, OpenOptions.defaults().setOffset(2));
      // check in back
      mManager.release(instream);
      Thread.sleep(10);
      // check out another stream should trigger the timeout
      mManager.acquire(mUfs, FILE_NAME, FILE_ID, OpenOptions.defaults().setOffset(4));
      verify(mSeekableInStreams[0], timeout(2000).times(1)).close();
    }
  }

  @Test
  public void releaseExpiredSameFile() throws Exception {
    try (Closeable r = new ConfigurationRule(new HashMap<PropertyKey, String>() {
      {
        put(PropertyKey.WORKER_UFS_INSTREAM_CACHE_EXPIRARTION_TIME, "2");
      }
    }, ServerConfiguration.global()).toResource()) {
      mManager = new UfsInputStreamCache();
      // check out a stream
      InputStream instream =
          mManager.acquire(mUfs, FILE_NAME, FILE_ID, OpenOptions.defaults().setOffset(2));
      // wait a bit for a stream to be expired
      Thread.sleep(100);
      // check out another stream should trigger the timeout
      mManager.acquire(mUfs, FILE_NAME, FILE_ID, OpenOptions.defaults().setOffset(4));

      // wait a bit so release occurs after removal listener
      Thread.sleep(100);
      mManager.release(instream);

      // verify the stream was closed once
      verify(mSeekableInStreams[0], timeout(2000).times(1)).close();
    }
  }

  @Test
  public void releaseExpiredDifferentFile() throws Exception {
    try (Closeable r = new ConfigurationRule(new HashMap<PropertyKey, String>() {
      {
        put(PropertyKey.WORKER_UFS_INSTREAM_CACHE_EXPIRARTION_TIME, "2");
      }
    }, ServerConfiguration.global()).toResource()) {
      mManager = new UfsInputStreamCache();
      // check out a stream
      InputStream instream =
          mManager.acquire(mUfs, FILE_NAME, FILE_ID, OpenOptions.defaults().setOffset(2));
      // wait a bit for a stream to be expired
      Thread.sleep(100);
      // check out another stream should trigger the timeout
      mManager.acquire(mUfs, FILE_NAME + "2", FILE_ID + 1, OpenOptions.defaults().setOffset(4));

      // wait a bit so release occurs after removal listener
      Thread.sleep(100);
      mManager.release(instream);

      // verify the stream was closed once
      verify(mSeekableInStreams[0], timeout(2000).times(1)).close();
    }
  }

  @Test
  public void concurrency() throws Exception {
    try (Closeable r = new ConfigurationRule(new HashMap<PropertyKey, String>() {
      {
        // use very large number
        put(PropertyKey.WORKER_UFS_INSTREAM_CACHE_EXPIRARTION_TIME, "200000");
      }
    }, ServerConfiguration.global()).toResource()) {
      mManager = new UfsInputStreamCache();
      List<Thread> threads = new ArrayList<>();
      int numCheckOutPerThread = 10;
      for (int i = 0; i < mNumOfInputStreams / 2; i++) {
        Runnable runnable = () -> {
          for (int j = 0; j < numCheckOutPerThread; j++) {
            InputStream instream;
            try {
              instream =
                  mManager.acquire(mUfs, FILE_NAME, FILE_ID, OpenOptions.defaults().setOffset(j));
              Thread.sleep(10);
              mManager.release(instream);
            } catch (Exception e) {
              // the input streams created should not be more than mNumOfInputStreams
              Assert.fail("input stream check in and out failed." + e);
            }
          }
        };
        threads.add(new Thread(runnable));
      }
      ConcurrencyUtils.assertConcurrent(threads, 30);
      // Each subsequent check out per thread should be a seek operation
      int numSeek = 0;
      for (int i = 0; i < mNumOfInputStreams; i++) {
        for (Invocation invocation : mockingDetails(mSeekableInStreams[i])
            .getInvocations()) {
          if (invocation.getMethod().getName().equals("seek")) {
            numSeek++;
          }
        }
      }
      Assert.assertEquals(mNumOfInputStreams / 2 * (numCheckOutPerThread - 1), numSeek);
    }
  }

  @Test
  public void concurrencyWithExpiration() throws Exception {
    try (Closeable r = new ConfigurationRule(new HashMap<PropertyKey, String>() {
      {
        put(PropertyKey.WORKER_UFS_INSTREAM_CACHE_EXPIRARTION_TIME, "20");
      }
    }, ServerConfiguration.global()).toResource()) {
      mManager = new UfsInputStreamCache();
      List<Thread> threads = new ArrayList<>();
      int numCheckOutPerThread = 4;
      for (int i = 0; i < mNumOfInputStreams / numCheckOutPerThread; i++) {
        Runnable runnable = () -> {
          for (int j = 0; j < numCheckOutPerThread; j++) {
            InputStream instream;
            try {
              instream =
                  mManager.acquire(mUfs, FILE_NAME, FILE_ID, OpenOptions.defaults().setOffset(j));
              mManager.release(instream);
              Thread.sleep(200);
            } catch (Exception e) {
              // the input streams created should not be more than mNumOfInputStreams
              Assert.fail("input stream check in and out failed." + e);
            }
          }
        };
        threads.add(new Thread(runnable));
      }
      ConcurrencyUtils.assertConcurrent(threads, 30);
      // ensure at least one expired in stream is closed
      verify(mSeekableInStreams[0], timeout(2000)).close();
    }
  }
}
