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

package alluxio.underfs.swift;

import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;

/**
 * Tests for the {@link SwiftOutputStream} class.
 */
@RunWith(PowerMockRunner.class)
public class SwiftOutputStreamTest {

  private OutputStream mOutputStreamMock;
  private HttpURLConnection mHttpConMock;

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
    mOutputStreamMock = PowerMockito.mock(OutputStream.class);
    mHttpConMock = PowerMockito.mock(HttpURLConnection.class);
    when(mHttpConMock.getOutputStream()).thenReturn(mOutputStreamMock);
  }

  /**
   * Tests to ensure IOException is thrown if {@link HttpURLConnection#getOutputStream()} throws an
   * IOException.
   */
  @Test
  public void testConstructor() throws Exception {
    String errorMessage = "protocol doesn't support output";
    when(mHttpConMock.getOutputStream()).thenThrow(new IOException(errorMessage));
    mThrown.expect(IOException.class);
    mThrown.expectMessage(errorMessage);
    new SwiftOutputStream(mHttpConMock).close();
  }

  /**
   * Tests to ensure {@link SwiftOutputStream#write(int)} calls {@link OutputStream#write(int)}.
   */
  @Test
  public void testWrite1() throws Exception {
    SwiftOutputStream stream = new SwiftOutputStream(mHttpConMock);
    stream.write(1);
    stream.close();
    verify(mOutputStreamMock).write(1);
  }

  /**
   * Tests to ensure {@link SwiftOutputStream#write(byte[], int, int)} calls
   * {@link OutputStream#write(byte[], int, int)} .
   */
  @Test
  public void testWrite2() throws Exception {
    SwiftOutputStream stream = new SwiftOutputStream(mHttpConMock);
    byte[] b = new byte[1];
    stream.write(b, 1, 1);
    stream.close();
    verify(mOutputStreamMock).write(b, 1, 1);
  }

  /**
   * Tests to ensure {@link SwiftOutputStream#write(byte[])} calls
   * {@link OutputStream#write(byte[])}.
   */
  @Test
  public void testWrite3() throws Exception {
    SwiftOutputStream stream = new SwiftOutputStream(mHttpConMock);
    byte[] b = new byte[1];
    stream.write(b);
    stream.close();
    verify(mOutputStreamMock).write(b);
  }

  /**
   * Tests to ensure {@link HttpURLConnection#getErrorStream()} is called when 400 is returned.
   */
  @Test
  public void testCloseError() throws Exception {
    when(mHttpConMock.getResponseCode()).thenReturn(400);
    SwiftOutputStream stream = new SwiftOutputStream(mHttpConMock);
    stream.close();
    verify(mHttpConMock).getErrorStream();
    verify(mHttpConMock).disconnect();
  }

  /**
   * Tests to ensure {@link HttpURLConnection#getInputStream()} is called when 200 is returned.
   */
  @Test
  public void testCloseSuccess() throws Exception {
    when(mHttpConMock.getResponseCode()).thenReturn(200);
    SwiftOutputStream stream = new SwiftOutputStream(mHttpConMock);
    stream.close();
    verify(mHttpConMock).getInputStream();
    verify(mHttpConMock).disconnect();
  }

  /**
   * Tests to ensure {@link SwiftOutputStream#flush()} calls {@link OutputStream#flush()}.
   */
  @Test
  public void testFlush() throws Exception {
    SwiftOutputStream stream = new SwiftOutputStream(mHttpConMock);
    stream.flush();
    stream.close();
    verify(mOutputStreamMock).flush();
  }
}
