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

package alluxio.underfs.tos;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;

import com.volcengine.tos.TOSV2;
import com.volcengine.tos.TosException;
import com.volcengine.tos.model.object.PutObjectInput;
import com.volcengine.tos.model.object.PutObjectOutput;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.security.DigestOutputStream;

/**
 * Unit tests for the {@link TOSOutputStream}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ TOSOutputStream.class, Files.class })
public class TOSOutputStreamTest {
  private TOSV2 mTosClient;
  private File mFile;
  private BufferedOutputStream mLocalOutputStream;
  private static AlluxioConfiguration sConf = Configuration.global();
  private final String mEtag = "someTag";

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  /**
   * Sets the properties and configuration before each test runs.
   */
  @Before
  public void before() throws Exception {
    mTosClient = Mockito.mock(TOSV2.class);
    PutObjectOutput result = Mockito.mock(PutObjectOutput.class);
    Mockito.when(result.getEtag()).thenReturn(mEtag);
    Mockito.when(mTosClient.putObject(any(PutObjectInput.class)))
        .thenReturn(result);
    mFile = Mockito.mock(File.class);
    PowerMockito.mockStatic(Files.class);
    mLocalOutputStream = Mockito.mock(BufferedOutputStream.class);
  }

  /**
   * Tests to ensure IOException is thrown if {@link FileOutputStream}() throws an IOException.
   */
  @Test
  public void testConstructor() throws Exception {
    PowerMockito.whenNew(File.class).withArguments(Mockito.anyString()).thenReturn(mFile);
    String errorMessage = "protocol doesn't support output";
    PowerMockito.when(Files.newOutputStream(mFile.toPath()))
        .thenThrow(new IOException(errorMessage));
    mThrown.expect(IOException.class);
    mThrown.expectMessage(errorMessage);
    new TOSOutputStream("testBucketName", "testKey", mTosClient,
      sConf.getList(PropertyKey.TMP_DIRS)).close();
  }

  /**
   * Tests to ensure {@link TOSOutputStream#write(int)} calls {@link OutputStream#write(int)}.
   */
  @Test
  public void testWrite1() throws Exception {
    PowerMockito.whenNew(BufferedOutputStream.class)
      .withArguments(any(DigestOutputStream.class)).thenReturn(mLocalOutputStream);
    PowerMockito.whenNew(BufferedOutputStream.class)
      .withArguments(any(FileOutputStream.class)).thenReturn(mLocalOutputStream);
    TOSOutputStream stream = new TOSOutputStream("testBucketName", "testKey", mTosClient,
        sConf.getList(PropertyKey.TMP_DIRS));
    stream.write(1);
    stream.close();
    assertEquals(mEtag, stream.getContentHash().get());
    Mockito.verify(mLocalOutputStream).write(1);
  }

  /**
   * Tests to ensure {@link TOSOutputStream#write(byte[], int, int)} calls
   * {@link OutputStream#write(byte[], int, int)} .
   */
  @Test
  public void testWrite2() throws Exception {
    PowerMockito.whenNew(BufferedOutputStream.class)
      .withArguments(any(DigestOutputStream.class)).thenReturn(mLocalOutputStream);
    PowerMockito.whenNew(BufferedOutputStream.class)
      .withArguments(any(FileOutputStream.class)).thenReturn(mLocalOutputStream);
    TOSOutputStream stream = new TOSOutputStream("testBucketName", "testKey", mTosClient,
        sConf.getList(PropertyKey.TMP_DIRS));
    byte[] b = new byte[1];
    stream.write(b, 0, 1);
    stream.close();
    assertEquals(mEtag, stream.getContentHash().get());
    Mockito.verify(mLocalOutputStream).write(b, 0, 1);
  }

  /**
   * Tests to ensure {@link TOSOutputStream#write(byte[])} calls {@link OutputStream#write(byte[])}.
   */
  @Test
  public void testWrite3() throws Exception {
    PowerMockito.whenNew(BufferedOutputStream.class)
      .withArguments(any(DigestOutputStream.class)).thenReturn(mLocalOutputStream);
    PowerMockito.whenNew(BufferedOutputStream.class)
      .withArguments(any(FileOutputStream.class)).thenReturn(mLocalOutputStream);
    TOSOutputStream stream = new TOSOutputStream("testBucketName", "testKey", mTosClient, sConf
        .getList(PropertyKey.TMP_DIRS));
    byte[] b = new byte[1];
    stream.write(b);
    stream.close();
    assertEquals(mEtag, stream.getContentHash().get());
    Mockito.verify(mLocalOutputStream).write(b, 0, 1);
  }

  /**
   * Tests to ensure IOException is thrown if
   * {@link TOSV2#putObject(PutObjectInput)} throws an
   * TosException.
   */
  @Test
  public void testCloseError() throws Exception {
    // Mock the necessary objects and their behaviors
    PowerMockito.whenNew(BufferedOutputStream.class)
      .withArguments(any(DigestOutputStream.class)).thenReturn(mLocalOutputStream);
    PowerMockito.whenNew(BufferedOutputStream.class)
      .withArguments(any(FileOutputStream.class)).thenReturn(mLocalOutputStream);

    // Simulate an exception thrown by the TOS client when attempting to put an object
    TosException simulatedException = new TosException();
    PowerMockito.when(mTosClient.putObject(any(PutObjectInput.class)))
        .thenThrow(simulatedException);

    // Instantiate the TOSOutputStream object with the mocked TOS client
    TOSOutputStream stream = new TOSOutputStream("testBucketName", "testKey", mTosClient,
        sConf.getList(PropertyKey.TMP_DIRS));

    // Set the expected exception type and message
    mThrown.expect(AlluxioTosException.class);

    // Attempt to close the stream, which should trigger the exception
    stream.close();

    // Verify that the content hash is still retrievable and matches the expected value
    assertEquals(mEtag, stream.getContentHash().get());
  }

  /**
   * Tests to ensure {@link File#delete()} is called when close the stream.
   */
  @Test
  public void testCloseSuccess() throws Exception {
    PowerMockito.whenNew(File.class).withArguments(Mockito.anyString()).thenReturn(mFile);
    PowerMockito.whenNew(BufferedOutputStream.class)
      .withArguments(any(DigestOutputStream.class)).thenReturn(mLocalOutputStream);
    PowerMockito.whenNew(BufferedOutputStream.class)
      .withArguments(any(FileOutputStream.class)).thenReturn(mLocalOutputStream);

    TOSOutputStream stream = new TOSOutputStream("testBucketName", "testKey", mTosClient, sConf
        .getList(PropertyKey.TMP_DIRS));
    stream.close();
    assertEquals(mEtag, stream.getContentHash().get());
    Mockito.verify(mFile).delete();
  }

  /**
   * Tests to ensure {@link TOSOutputStream#flush()} calls {@link OutputStream#flush()}.
   */
  @Test
  public void testFlush() throws Exception {
    PowerMockito.whenNew(BufferedOutputStream.class)
      .withArguments(any(DigestOutputStream.class)).thenReturn(mLocalOutputStream);
    TOSOutputStream stream = new TOSOutputStream("testBucketName", "testKey", mTosClient, sConf
        .getList(PropertyKey.TMP_DIRS));
    stream.flush();
    stream.close();
    assertEquals(mEtag, stream.getContentHash().get());
    Mockito.verify(mLocalOutputStream).flush();
  }
}

