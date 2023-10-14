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

package alluxio.underfs.gcs;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;

import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.GoogleStorageService;
import org.jets3t.service.model.GSObject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.DigestOutputStream;

/**
 * Unit tests for the {@link GCSOutputStream}.
 */
@RunWith(PowerMockRunner.class)
public class GCSOutputStreamTest {
  private GoogleStorageService mClient;
  private File mFile;
  private BufferedOutputStream mLocalOutputStream;
  private static AlluxioConfiguration sConf = Configuration.global();
  private GSObject mObject;
  private String mMd5Hash = "md5 hash";

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
    mClient = Mockito.mock(GoogleStorageService.class);
    mObject = Mockito.mock(GSObject.class);
    Mockito.when(mObject.getMd5HashAsBase64()).thenReturn(mMd5Hash);
    Mockito.when(mClient.putObject(Mockito.anyString(), Mockito.any(GSObject.class)))
        .thenReturn(mObject);
    mFile = Mockito.mock(File.class);
    mLocalOutputStream = Mockito.mock(BufferedOutputStream.class);
  }

  /**
   * Tests to ensure IOException is thrown if {@link FileOutputStream}() throws an IOException.
   */
  @Test
  @PrepareForTest(GCSOutputStream.class)
  public void testConstructor() throws Exception {
    PowerMockito.whenNew(File.class).withArguments(Mockito.anyString()).thenReturn(mFile);
    String errorMessage = "protocol doesn't support output";
    PowerMockito.whenNew(FileOutputStream.class).withArguments(mFile)
        .thenThrow(new IOException(errorMessage));
    mThrown.expect(IOException.class);
    mThrown.expectMessage(errorMessage);
    new GCSOutputStream("testBucketName", "testKey", mClient,
        sConf.getList(PropertyKey.TMP_DIRS)).close();
  }

  /**
   * Tests to ensure {@link GCSOutputStream#write(int)} calls {@link OutputStream#write(int)}.
   */
  @Test
  @PrepareForTest(GCSOutputStream.class)
  public void testWrite1() throws Exception {
    PowerMockito.whenNew(BufferedOutputStream.class)
        .withArguments(any(DigestOutputStream.class)).thenReturn(mLocalOutputStream);
    PowerMockito.whenNew(BufferedOutputStream.class)
        .withArguments(any(FileOutputStream.class)).thenReturn(mLocalOutputStream);
    GCSOutputStream stream = new GCSOutputStream("testBucketName", "testKey", mClient,
        sConf.getList(PropertyKey.TMP_DIRS));
    stream.write(1);
    stream.close();
    assertEquals(mMd5Hash, stream.getContentHash().get());
    Mockito.verify(mLocalOutputStream).write(1);
  }

  /**
   * Tests to ensure {@link GCSOutputStream#write(byte[], int, int)} calls
   * {@link OutputStream#write(byte[], int, int)} .
   */
  @Test
  @PrepareForTest(GCSOutputStream.class)
  public void testWrite2() throws Exception {
    PowerMockito.whenNew(BufferedOutputStream.class)
        .withArguments(any(DigestOutputStream.class)).thenReturn(mLocalOutputStream);
    PowerMockito.whenNew(BufferedOutputStream.class)
        .withArguments(any(FileOutputStream.class)).thenReturn(mLocalOutputStream);
    GCSOutputStream stream = new GCSOutputStream("testBucketName", "testKey", mClient,
        sConf.getList(PropertyKey.TMP_DIRS));
    byte[] b = new byte[1];
    stream.write(b, 0, 1);
    stream.close();
    assertEquals(mMd5Hash, stream.getContentHash().get());
    Mockito.verify(mLocalOutputStream).write(b, 0, 1);
  }

  /**
   * Tests to ensure {@link GCSOutputStream#write(byte[])} calls {@link OutputStream#write(byte[])}.
   */
  @Test
  @PrepareForTest(GCSOutputStream.class)
  public void testWrite3() throws Exception {
    PowerMockito.whenNew(BufferedOutputStream.class)
        .withArguments(any(DigestOutputStream.class)).thenReturn(mLocalOutputStream);
    PowerMockito.whenNew(BufferedOutputStream.class)
        .withArguments(any(FileOutputStream.class)).thenReturn(mLocalOutputStream);
    GCSOutputStream stream = new GCSOutputStream("testBucketName", "testKey", mClient, sConf
        .getList(PropertyKey.TMP_DIRS));
    byte[] b = new byte[1];
    stream.write(b);
    stream.close();
    assertEquals(mMd5Hash, stream.getContentHash().get());
    Mockito.verify(mLocalOutputStream).write(b, 0, 1);
  }

  /**
   * Tests to ensure IOException is thrown if
   * {@link GoogleStorageService#putObject(String, GSObject)} throws an
   * ServiceException.
   */
  @Test
  @PrepareForTest(GCSOutputStream.class)
  public void testCloseError() throws Exception {
    String errorMessage = "Invoke the createEmptyObject method error.";
    BufferedInputStream inputStream = PowerMockito.mock(BufferedInputStream.class);
    PowerMockito.whenNew(BufferedInputStream.class)
        .withArguments(any(FileInputStream.class)).thenReturn(inputStream);
    PowerMockito
        .when(mClient.putObject(Mockito.anyString(), Mockito.any(GSObject.class)))
        .thenThrow(new ServiceException(errorMessage));
    GCSOutputStream stream = new GCSOutputStream("testBucketName", "testKey", mClient, sConf
        .getList(PropertyKey.TMP_DIRS));
    mThrown.expect(IOException.class);
    mThrown.expectMessage(errorMessage);
    stream.close();
    assertEquals(mMd5Hash, stream.getContentHash().get());
  }

  /**
   * Tests to ensure {@link File#delete()} is called when close the stream.
   */
  @Test
  @PrepareForTest(GCSOutputStream.class)
  public void testCloseSuccess() throws Exception {
    PowerMockito.whenNew(File.class).withArguments(Mockito.anyString()).thenReturn(mFile);
    FileOutputStream outputStream = PowerMockito.mock(FileOutputStream.class);
    PowerMockito.whenNew(FileOutputStream.class).withArguments(mFile).thenReturn(outputStream);
    FileInputStream inputStream = PowerMockito.mock(FileInputStream.class);
    PowerMockito.whenNew(FileInputStream.class).withArguments(mFile).thenReturn(inputStream);

    GCSOutputStream stream = new GCSOutputStream("testBucketName", "testKey", mClient, sConf
        .getList(PropertyKey.TMP_DIRS));
    stream.close();
    assertEquals(mMd5Hash, stream.getContentHash().get());
    Mockito.verify(mFile).delete();
  }

  /**
   * Tests to ensure {@link GCSOutputStream#flush()} calls {@link OutputStream#flush()}.
   */
  @Test
  @PrepareForTest(GCSOutputStream.class)
  public void testFlush() throws Exception {
    PowerMockito.whenNew(BufferedOutputStream.class)
        .withArguments(any(DigestOutputStream.class)).thenReturn(mLocalOutputStream);
    GCSOutputStream stream = new GCSOutputStream("testBucketName", "testKey", mClient, sConf
        .getList(PropertyKey.TMP_DIRS));
    stream.flush();
    stream.close();
    assertEquals(mMd5Hash, stream.getContentHash().get());
    Mockito.verify(mLocalOutputStream).flush();
  }
}
