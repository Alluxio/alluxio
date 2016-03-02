/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.hadoop;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.spy;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.ExceptionMessage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.util.Random;

/**
 * Unit tests for {@link HdfsFileInputStream}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({HdfsFileInputStream.class, AlluxioURI.class,
    Path.class, Configuration.class, Statistics.class, FileSystem.class,
    URIStatus.class, FSDataInputStream.class, FileInStream.class})
public class HdfsFileInputStreamTest {
  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  private static final int BUFSIZE = 1024;
  private static final Random RANDOM = new Random();

  private HdfsFileInputStream mHdfsFileInputStream;
  private HdfsFileInputStream mSpy;
  private AlluxioURI mAlluxioURI;
  private Path mHdfsPath;
  private org.apache.hadoop.conf.Configuration mHdfsConf;
  private org.apache.hadoop.fs.FileSystem.Statistics mStats;
  private URIStatus mFileInfo;
  private FSDataInputStream mHdfsInputStream;
  private FileInStream mAlluxioFileInputStream;

  /**
   * Sets up the context before a test runs.
   *
   * @throws Exception when the initialize fail
   */
  @Before
  public void before() throws Exception {
    mAlluxioURI = PowerMockito.mock(AlluxioURI.class);
    mHdfsPath = PowerMockito.mock(Path.class);
    mHdfsConf = PowerMockito.mock(org.apache.hadoop.conf.Configuration.class);
    mStats = PowerMockito.mock(org.apache.hadoop.fs.FileSystem.Statistics.class);
    mFileInfo = PowerMockito.mock(URIStatus.class);
    mHdfsInputStream = PowerMockito.mock(FSDataInputStream.class);
    mAlluxioFileInputStream = PowerMockito.mock(FileInStream.class);

    PowerMockito.mockStatic(FileSystem.Factory.class);
    FileSystem fs = PowerMockito.mock(FileSystem.class);
    when(FileSystem.Factory.get()).thenReturn(fs);

    when(fs.getStatus(mAlluxioURI)).thenReturn(mFileInfo);
    when(fs.openFile(any(AlluxioURI.class), any(OpenFileOptions.class)))
      .thenReturn(mAlluxioFileInputStream);

    org.apache.hadoop.fs.FileSystem dfs = PowerMockito.mock(org.apache.hadoop.fs.FileSystem.class);
    when(mHdfsPath.getFileSystem(any(org.apache.hadoop.conf.Configuration.class))).thenReturn(dfs);
    when(dfs.open(any(Path.class), anyInt())).thenReturn(mHdfsInputStream);

    mHdfsFileInputStream = new HdfsFileInputStream(
        mAlluxioURI, mHdfsPath, mHdfsConf, BUFSIZE, mStats);
    Whitebox.setInternalState(mHdfsFileInputStream, "mHdfsInputStream", mHdfsInputStream);
    mSpy = spy(mHdfsFileInputStream);
  }

  /**
   * Resets the test context.
   *
   * @throws IOException when the initialize fail
   */
  @After
  public void after() throws IOException {
    mHdfsFileInputStream.close();
  }

  /**
   * Tests that {@link HdfsFileInputStream#available()} throws the right exception.
   *
   * @throws IOException Should throw IOException
   */
  @Test
  public void availableTest() throws IOException {
    mThrown.expect(IOException.class);
    mThrown.expectMessage(ExceptionMessage.NOT_SUPPORTED.getMessage());
    mSpy.available();
  }

  /**
   * Tests {@link HdfsFileInputStream#getPos()}.
   *
   * @throws IOException if getPos call fails
   */
  @Test
  public void getPosTest() throws IOException {
    long pos = RANDOM.nextLong();
    doReturn(pos).when(mSpy).getPos();
    mSpy.getPos();
    verify(mSpy).getPos();
  }

  /**
   * Tests {@link HdfsFileInputStream#read()}.
   *
   * @throws IOException if the read call fails
   */
  @Test
  public void readTest1() throws IOException {
    when(mAlluxioFileInputStream.read()).thenReturn(1);
    when(mHdfsInputStream.read(any(byte[].class))).thenReturn(5);
    mSpy.read();
    verify(mSpy).read();
    long pos = Whitebox.getInternalState(mSpy, "mCurrentPosition");
    assertEquals(pos, 1);
  }

  /**
   * Tests that {@link HdfsFileInputStream#read(byte[])} throws the right exception.
   *
   * @throws IOException Should throw IOException
   */
  @Test
  public void readTest2() throws IOException {
    byte[] b = new byte[20];
    RANDOM.nextBytes(b);
    mThrown.expect(IOException.class);
    mThrown.expectMessage(ExceptionMessage.NOT_SUPPORTED.getMessage());
    mSpy.read(b);
  }

  /**
   * Tests that {@link HdfsFileInputStream#readFully(long, byte[])} throws the right exception.
   *
   * @throws IOException Should throw IOException
   */
  @Test
  public void readFullyTest() throws IOException {
    byte[] b = new byte[20];
    RANDOM.nextBytes(b);
    long position = RANDOM.nextLong();
    mThrown.expect(IOException.class);
    mThrown.expectMessage(ExceptionMessage.NOT_SUPPORTED.getMessage());
    mSpy.readFully(position, b);
  }

  /**
   * Tests that {@link HdfsFileInputStream#readFully(long, byte[], int, int)}
   * throws the right exception.
   *
   * @throws IOException Should throw IOException
   */
  @Test
  public void readFullyTest2() throws IOException {
    byte[] b = new byte[20];
    RANDOM.nextBytes(b);
    long position = RANDOM.nextLong();
    int offset = RANDOM.nextInt();
    int length = RANDOM.nextInt();
    mThrown.expect(IOException.class);
    mThrown.expectMessage(ExceptionMessage.NOT_SUPPORTED.getMessage());
    mSpy.readFully(position, b, offset, length);
  }

  /**
   * Tests the {@link HdfsFileInputStream#seek(long)}.
   *
   * @throws IOException if the seek call fails
   */
  @Test
  public void seekTest() throws IOException {
    long curPos = Whitebox.getInternalState(mSpy, "mCurrentPosition");
    long targetPos = RANDOM.nextLong();
    long fileLength = RANDOM.nextInt(Integer.MAX_VALUE);
    when(mFileInfo.getLength()).thenReturn(fileLength);
    if (targetPos == curPos) {
      doNothing().when(mSpy).seek(targetPos);
    } else if (targetPos < 0) {
      mThrown.expect(IOException.class);
      mThrown.expectMessage(ExceptionMessage.SEEK_NEGATIVE.getMessage(targetPos));
      mSpy.seek(targetPos);
    } else if (targetPos > fileLength) {
      mThrown.expect(IOException.class);
      mThrown.expectMessage(ExceptionMessage.SEEK_PAST_EOF.getMessage(targetPos, fileLength));
      mSpy.seek(targetPos);
    } else {
      mSpy.seek(targetPos);
      verify(mSpy).seek(targetPos);
      curPos = Whitebox.getInternalState(mSpy, "mCurrentPosition");
      assertEquals(curPos, targetPos);
    }
  }

  /**
   * Tests that {@link HdfsFileInputStream#seekToNewSource(long)} throws the right exception.
   *
   * @throws IOException Should throw IOException
   */
  @Test
  public void seekToNewSourceTest() throws IOException {
    long targetPos = RANDOM.nextLong();
    mThrown.expect(IOException.class);
    mThrown.expectMessage(ExceptionMessage.NOT_SUPPORTED.getMessage());
    mSpy.seekToNewSource(targetPos);
  }
}
