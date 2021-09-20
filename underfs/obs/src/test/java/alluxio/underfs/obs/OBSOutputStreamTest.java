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

package alluxio.underfs.obs;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.ObjectMetadata;
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
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestOutputStream;

/**
 * Unit tests for the {@link OBSOutputStream}.
 */
@RunWith(PowerMockRunner.class)
public class OBSOutputStreamTest {
  private static AlluxioConfiguration sConf =
      new InstancedConfiguration(ConfigurationUtils.defaults());

  private ObsClient mObsClient;
  private File mFile;
  private BufferedOutputStream mLocalOutputStream;

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
    mObsClient = Mockito.mock(ObsClient.class);
    mFile = Mockito.mock(File.class);
    mLocalOutputStream = Mockito.mock(BufferedOutputStream.class);
  }

  /**
   * Tests to ensure IOException is thrown if {@link FileOutputStream()} throws an IOException.
   */
  @Test
  @PrepareForTest(OBSOutputStream.class)
  public void testConstructor() throws Exception {
    PowerMockito.whenNew(File.class).withArguments(Mockito.anyString()).thenReturn(mFile);
    String errorMessage = "protocol doesn't support output";
    PowerMockito.whenNew(FileOutputStream.class).withArguments(mFile)
        .thenThrow(new IOException(errorMessage));
    mThrown.expect(IOException.class);
    mThrown.expectMessage(errorMessage);
    new OBSOutputStream("testBucketName", "testKey", mObsClient,
        sConf.getList(PropertyKey.TMP_DIRS, ",")).close();
  }

  /**
   * Tests to ensure {@link OBSOutputStream#write(int)} calls {@link OutputStream#write(int)}.
   */
  @Test
  @PrepareForTest(OBSOutputStream.class)
  public void testWrite1() throws Exception {
    PowerMockito.whenNew(BufferedOutputStream.class)
        .withArguments(Mockito.any(DigestOutputStream.class)).thenReturn(mLocalOutputStream);
    PowerMockito.whenNew(BufferedOutputStream.class)
        .withArguments(Mockito.any(FileOutputStream.class)).thenReturn(mLocalOutputStream);
    OBSOutputStream stream = new OBSOutputStream("testBucketName", "testKey", mObsClient,
        sConf.getList(PropertyKey.TMP_DIRS, ","));
    stream.write(1);
    stream.close();
    Mockito.verify(mLocalOutputStream).write(1);
  }

  /**
   * Tests to ensure {@link OBSOutputStream#write(byte[], int, int)} calls
   * {@link OutputStream#write(byte[], int, int)} .
   */
  @Test
  @PrepareForTest(OBSOutputStream.class)
  public void testWrite2() throws Exception {
    PowerMockito.whenNew(BufferedOutputStream.class)
        .withArguments(Mockito.any(DigestOutputStream.class)).thenReturn(mLocalOutputStream);
    PowerMockito.whenNew(BufferedOutputStream.class)
        .withArguments(Mockito.any(FileOutputStream.class)).thenReturn(mLocalOutputStream);
    OBSOutputStream stream = new OBSOutputStream("testBucketName", "testKey", mObsClient,
        sConf.getList(PropertyKey.TMP_DIRS, ","));
    byte[] b = new byte[1];
    stream.write(b, 0, 1);
    stream.close();
    Mockito.verify(mLocalOutputStream).write(b, 0, 1);
  }

  /**
   * Tests to ensure {@link OBSOutputStream#write(byte[])} calls {@link OutputStream#write(byte[])}.
   */
  @Test
  @PrepareForTest(OBSOutputStream.class)
  public void testWrite3() throws Exception {
    PowerMockito.whenNew(BufferedOutputStream.class)
        .withArguments(Mockito.any(DigestOutputStream.class)).thenReturn(mLocalOutputStream);
    PowerMockito.whenNew(BufferedOutputStream.class)
        .withArguments(Mockito.any(FileOutputStream.class)).thenReturn(mLocalOutputStream);
    OBSOutputStream stream = new OBSOutputStream("testBucketName", "testKey", mObsClient,
        sConf.getList(PropertyKey.TMP_DIRS, ","));
    byte[] b = new byte[1];
    stream.write(b);
    stream.close();
    Mockito.verify(mLocalOutputStream).write(b, 0, 1);
  }

  /**
   * Tests to ensure IOException is thrown if
   * {@link ObsClient#putObject(String, String, InputStream, ObjectMetadata)} throws an
   * {@link ObsException}.
   */
  @Test
  @PrepareForTest(OBSOutputStream.class)
  public void testCloseError() throws Exception {
    String errorMessage = "Invoke the createEmptyObject method error.";
    BufferedInputStream inputStream = PowerMockito.mock(BufferedInputStream.class);
    PowerMockito.whenNew(BufferedInputStream.class)
        .withArguments(Mockito.any(FileInputStream.class)).thenReturn(inputStream);
    PowerMockito
        .when(mObsClient.putObject(Mockito.anyString(), Mockito.anyString(),
            Mockito.any(InputStream.class), Mockito.any(ObjectMetadata.class)))
        .thenThrow(new ObsException(errorMessage));
    OBSOutputStream stream = new OBSOutputStream("testBucketName", "testKey", mObsClient,
        sConf.getList(PropertyKey.TMP_DIRS, ","));
    mThrown.expect(IOException.class);
    mThrown.expectMessage(errorMessage);
    stream.close();
  }

  /**
   * Tests to ensure {@link File#delete()} is called when close the stream.
   */
  @Test
  @PrepareForTest(OBSOutputStream.class)
  public void testCloseSuccess() throws Exception {
    PowerMockito.whenNew(File.class).withArguments(Mockito.anyString()).thenReturn(mFile);
    FileOutputStream outputStream = PowerMockito.mock(FileOutputStream.class);
    PowerMockito.whenNew(FileOutputStream.class).withArguments(mFile).thenReturn(outputStream);
    FileInputStream inputStream = PowerMockito.mock(FileInputStream.class);
    PowerMockito.whenNew(FileInputStream.class).withArguments(mFile).thenReturn(inputStream);

    OBSOutputStream stream = new OBSOutputStream("testBucketName", "testKey", mObsClient,
        sConf.getList(PropertyKey.TMP_DIRS, ","));
    stream.close();
    Mockito.verify(mFile).delete();
  }

  /**
   * Tests to ensure {@link OBSOutputStream#flush()} calls {@link OutputStream#flush()}.
   */
  @Test
  @PrepareForTest(OBSOutputStream.class)
  public void testFlush() throws Exception {
    PowerMockito.whenNew(BufferedOutputStream.class)
        .withArguments(Mockito.any(DigestOutputStream.class)).thenReturn(mLocalOutputStream);
    OBSOutputStream stream = new OBSOutputStream("testBucketName", "testKey", mObsClient,
        sConf.getList(PropertyKey.TMP_DIRS, ","));
    stream.flush();
    stream.close();
    Mockito.verify(mLocalOutputStream).flush();
  }
}
