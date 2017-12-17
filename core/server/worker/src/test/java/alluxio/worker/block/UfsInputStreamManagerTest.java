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

import alluxio.ConcurrencyTestUtils;
import alluxio.ConfigurationRule;
import alluxio.PropertyKey;
import alluxio.underfs.SeekableUnderFileInputStream;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.OpenOptions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.Closeable;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Unit tests for {@link UfsInputStreamManager}.
 */
public final class UfsInputStreamManagerTest {
  private static final String FILE_NAME = "/test";

  private UnderFileSystem mUfs;
  private SeekableUnderFileInputStream[] mSeekableInStreams;
  private UfsInputStreamManager mManager;
  private int mNumOfInputStreams = 20;

  @Before
  public void before() throws Exception {
    mSeekableInStreams = new SeekableUnderFileInputStream[mNumOfInputStreams];
    mUfs = Mockito.mock(UnderFileSystem.class);
    Mockito.when(mUfs.isSeekable()).thenReturn(true);
    for (int i = 0; i < mNumOfInputStreams; i++) {
      SeekableUnderFileInputStream instream = createMockedSeekableStream();
      mSeekableInStreams[i] = instream;
    }
    Mockito.when(mUfs.open(Mockito.eq(FILE_NAME), Mockito.any(OpenOptions.class))).thenReturn(
        mSeekableInStreams[0], Arrays.copyOfRange(mSeekableInStreams, 1, mNumOfInputStreams));
    mManager = new UfsInputStreamManager();
  }

  private SeekableUnderFileInputStream createMockedSeekableStream() {
    SeekableUnderFileInputStream instream = Mockito.mock(SeekableUnderFileInputStream.class);
    Mockito.when(instream.getFilePath()).thenReturn(FILE_NAME);
    Mockito.doCallRealMethod().when(instream).setResourceId(Mockito.any(Long.class));
    Mockito.when(instream.getResourceId()).thenCallRealMethod();
    return instream;
  }

  /**
   * Test that verifies a checked-in input stream can be reused for the next check out.
   */
  @Test
  public void testCheckInAndOut() throws Exception {
    SeekableUnderFileInputStream mockedStrem = createMockedSeekableStream();
    Mockito.when(mUfs.open(Mockito.eq(FILE_NAME), Mockito.any(OpenOptions.class)))
        .thenReturn(mockedStrem).thenThrow(new IllegalStateException("Should only be called once"));

    // check out a stream
    InputStream instream1 = mManager.checkOut(mUfs, FILE_NAME, 2);
    // check in back
    mManager.checkIn(instream1);
    // check out a stream again
    InputStream instream2 = mManager.checkOut(mUfs, FILE_NAME, 4);

    Assert.assertEquals(mockedStrem, instream1);
    Assert.assertEquals(mockedStrem, instream2);
    // ensure the second time the checked out instream is the same one but repositioned
    Mockito.verify(mockedStrem).seek(4);
  }

  /**
   * Tests checking out multiple times will open new input streams.
   */
  @Test
  public void testMultipleCheckIn() throws Exception {
    mManager.checkOut(mUfs, FILE_NAME, 2);
    mManager.checkOut(mUfs, FILE_NAME, 4);
    mManager.checkOut(mUfs, FILE_NAME, 6);
    // 3 different input streams are checked out
    Mockito.verify(mUfs, Mockito.times(3)).open(Mockito.eq(FILE_NAME),
        Mockito.any(OpenOptions.class));
  }

  /**
   * Tests the input stream is closed after the resource is expired.
   */
  @Test
  public void testExpire() throws Exception {
    try (Closeable r = new ConfigurationRule(new HashMap<PropertyKey, String>() {
      {
        put(PropertyKey.WORKER_UFS_INSTREAM_CACHE_EXPIRE_MS, "2");
      }
    }).toResource()) {
      mManager = new UfsInputStreamManager();
      // check out a stream
      InputStream instream = mManager.checkOut(mUfs, FILE_NAME, 2);
      // check in back
      mManager.checkIn(instream);
      Thread.sleep(10);
      // check out another stream should trigger the timeout
      mManager.checkOut(mUfs, FILE_NAME, 4);
      Mockito.verify(mSeekableInStreams[0], Mockito.timeout(2000).times(1)).close();
    }
  }

  /**
   * Tests concurrent checkouts without expiration do not hit deadlock.
   */
  @Test
  public void testConcurrency() throws Exception {
    mManager = new UfsInputStreamManager();
    List<Thread> threads = new ArrayList<>();
    int numCheckOutPerThread = 10;
    for (int i = 0; i < mNumOfInputStreams / 2; i++) {
      Runnable runnable = () -> {
        for (int j = 0; j < numCheckOutPerThread; j++) {
          InputStream instream;
          try {
            instream = mManager.checkOut(mUfs, FILE_NAME, j);
            Thread.sleep(10);
            mManager.checkIn(instream);
          } catch (Exception e) {
            // the input streams created should not be more than mNumOfInputStreams
            Assert.fail("input stream check in and out failed." + e);
          }
        }
      };
      threads.add(new Thread(runnable));
    }
    ConcurrencyTestUtils.assertConcurrent(threads, 30);
    // Each subsequent check out per thread should be a seek operation
    Mockito.verify(mSeekableInStreams[0], Mockito.times(numCheckOutPerThread - 1))
        .seek(Mockito.anyLong());
  }

  /**
   * Tests concurrent checkouts with expiration do not hit deadlock.
   */
  @Test
  public void testConcurrencyWithExpiration() throws Exception {
    try (Closeable r = new ConfigurationRule(new HashMap<PropertyKey, String>() {
      {
        put(PropertyKey.WORKER_UFS_INSTREAM_CACHE_EXPIRE_MS, "2");
      }
    }).toResource()) {
      mManager = new UfsInputStreamManager();
      List<Thread> threads = new ArrayList<>();
      int numCheckOutPerThread = 2;
      for (int i = 0; i < mNumOfInputStreams / 2; i++) {
        Runnable runnable = () -> {
          for (int j = 0; j < numCheckOutPerThread; j++) {
            InputStream instream;
            try {
              instream = mManager.checkOut(mUfs, FILE_NAME, j);
              mManager.checkIn(instream);
              Thread.sleep(100);
            } catch (Exception e) {
              // the input streams created should not be more than mNumOfInputStreams
              Assert.fail("input stream check in and out failed." + e);
            }
          }
        };
        threads.add(new Thread(runnable));
      }
      ConcurrencyTestUtils.assertConcurrent(threads, 30);
      for (int i = 0; i < mNumOfInputStreams / 2; i++) {
        // the first half of input streams are closed
        Mockito.verify(mSeekableInStreams[i]).close();
      }
    }
  }
}
