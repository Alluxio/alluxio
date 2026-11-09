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

package alluxio.proxy.s3;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;

/** Response framing must describe the bytes actually written to the servlet. */
public class S3HandlerTest {
  @Test
  public void asciiStringBytes() throws Exception {
    assertStringBytes("<ListBucketResult><Key>ordinary</Key></ListBucketResult>");
  }

  @Test
  public void unicodeStringBytes() throws Exception {
    assertStringBytes("<ListBucketResult><Key>雪🚀</Key></ListBucketResult>");
  }

  private void assertStringBytes(String content) throws Exception {
    byte[] expected = content.getBytes(StandardCharsets.UTF_8);
    Response response = response(content);
    HttpServletResponse servlet = mock(HttpServletResponse.class);
    ServletOutputStream output = mock(ServletOutputStream.class);
    when(servlet.getOutputStream()).thenReturn(output);
    S3Handler.processResponse(servlet, response);
    verify(servlet).setContentLength(expected.length);
    ArgumentCaptor<byte[]> written = ArgumentCaptor.forClass(byte[].class);
    verify(output).write(written.capture());
    assertArrayEquals(expected, written.getValue());
    verify(response).close();
  }

  @Test
  public void streamBytesKeepExistingFraming() throws Exception {
    byte[] expected = new byte[17001];
    for (int i = 0; i < expected.length; i++) {
      expected[i] = (byte) i;
    }
    Response response = response(new ByteArrayInputStream(expected));
    HttpServletResponse servlet = mock(HttpServletResponse.class);
    ServletOutputStream output = mock(ServletOutputStream.class);
    ByteArrayOutputStream written = new ByteArrayOutputStream();
    when(servlet.getOutputStream()).thenReturn(output);
    doAnswer(invocation -> {
      written.write(invocation.getArgument(0), invocation.getArgument(1),
          invocation.getArgument(2));
      return null;
    }).when(output).write(any(byte[].class), anyInt(), anyInt());
    S3Handler.processResponse(servlet, response);
    assertArrayEquals(expected, written.toByteArray());
    verify(servlet, never()).setContentLength(anyInt());
    verify(response).close();
  }

  @Test
  public void outputFailureStillClosesResponse() throws Exception {
    Response response = response("雪");
    HttpServletResponse servlet = mock(HttpServletResponse.class);
    ServletOutputStream output = mock(ServletOutputStream.class);
    IOException failure = new IOException("test output failure");
    when(servlet.getOutputStream()).thenReturn(output);
    doThrow(failure).when(output).write(any(byte[].class));
    try {
      S3Handler.processResponse(servlet, response);
      fail("output failure was swallowed");
    } catch (IOException expected) {
      assertSame(failure, expected);
    }
    verify(response).close();
  }

  private Response response(Object entity) {
    Response response = mock(Response.class);
    when(response.getStatus()).thenReturn(200);
    when(response.getStringHeaders()).thenReturn(new MultivaluedHashMap<>());
    when(response.hasEntity()).thenReturn(true);
    when(response.getEntity()).thenReturn(entity);
    return response;
  }
}
