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

package alluxio.underfs.swift;

import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

import org.junit.Before;
import org.junit.Test;
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

  private static OutputStream sOutputStreamMock;
  private static HttpURLConnection sHttpConMock;

  /**
   * Sets the properties and configuration before each test runs.
   *
   * @throws Exception when the getOutputStream throws exception
   */
  @Before
  public void before() throws Exception {
    sOutputStreamMock = PowerMockito.mock(OutputStream.class);
    sHttpConMock = PowerMockito.mock(HttpURLConnection.class);
    when(sHttpConMock.getOutputStream()).thenReturn(sOutputStreamMock);
  }

  /**
   * Tests to ensure IOException is thrown if HttpURLConnection throws an IOException.
   *
   * @throws Exception when the IOException is not thrown
   */
  @Test(expected = IOException.class)
  public void testConstructor() throws Exception {
    when(sHttpConMock.getOutputStream()).thenThrow(new IOException());
    SwiftOutputStream stream = new SwiftOutputStream(sHttpConMock);
  }

  /**
   * Tests to ensure write calls OutputStream.write().
   *
   * @throws Exception when write is not called
   */
  @Test
  public void testWrite() throws Exception {
    SwiftOutputStream stream = new SwiftOutputStream(sHttpConMock);
    stream.write(1);
    verify(sOutputStreamMock).write(1);
  }

  /**
   * Tests to ensure write calls OutputStream.write().
   *
   * @throws Exception when write is not called
   */
  @Test
  public void testWrite1() throws Exception {
    SwiftOutputStream stream = new SwiftOutputStream(sHttpConMock);
    byte[] b = new byte[1];
    stream.write(b, 1, 1);
    verify(sOutputStreamMock).write(b, 1, 1);
  }

  /**
   * Tests to ensure write calls OutputStream.write().
   *
   * @throws Exception when write is not called
   */
  @Test
  public void testWrite2() throws Exception {
    SwiftOutputStream stream = new SwiftOutputStream(sHttpConMock);
    byte[] b = new byte[1];
    stream.write(b);
    verify(sOutputStreamMock).write(b);
  }

  /**
   * Tests to ensure getErrorStream() is called when 400 is returned.
   *
   * @throws Exception when getErrorStream() and disconnect() are not called
   */
  @Test
  public void testCloseError() throws Exception {
    when(sHttpConMock.getResponseCode()).thenReturn(400);
    SwiftOutputStream stream = new SwiftOutputStream(sHttpConMock);
    stream.close();
    verify(sHttpConMock).getErrorStream();
    verify(sHttpConMock).disconnect();
  }

  /**
   * Tests to ensure getInputStream() is called when 200 is returned.
   *
   * @throws Exception when getInputStream() and disconnect() are not called
   */
  @Test
  public void testCloseSuccess() throws Exception {
    when(sHttpConMock.getResponseCode()).thenReturn(200);
    SwiftOutputStream stream = new SwiftOutputStream(sHttpConMock);
    stream.close();
    verify(sHttpConMock).getInputStream();
    verify(sHttpConMock).disconnect();
  }

  /**
   * Tests to ensure flush calls OutputStream.flush().
   *
   * @throws Exception when flush is not called
   */
  @Test
  public void testFlush() throws Exception {
    SwiftOutputStream stream = new SwiftOutputStream(sHttpConMock);
    stream.flush();
    verify(sOutputStreamMock).flush();
  }
}
