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

package alluxio.hub.agent.util.process;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.TestLoggerRule;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class KillTest {

  @Rule
  public TestLoggerRule mLogger = new TestLoggerRule();

  @Test
  public void testKillSignal() throws Exception {
    try (MockedStatic<Runtime> mock = Mockito.mockStatic(Runtime.class)) {
      Runtime runtimeMock = mock(Runtime.class);
      Process processMock = mock(Process.class);
      when(processMock.getInputStream()).thenReturn(new ByteArrayInputStream(new byte[0]));
      when(processMock.getErrorStream()).thenReturn(new ByteArrayInputStream(new byte[0]));
      when(processMock.getOutputStream()).thenReturn(mock(OutputStream.class));
      when(processMock.exitValue()).thenReturn(0);
      mock.when(Runtime::getRuntime).thenReturn(runtimeMock);
      when(runtimeMock.exec(ArgumentMatchers.anyString())).thenReturn(processMock);
      int pid = 1234;
      Kill.kill(pid);
      verify(runtimeMock).exec(
          String.format("kill -%d %d", Kill.Signal.SIGTERM.getSigNum(), pid));
    }
  }

  @Test
  public void testKillSignalFail() throws Exception {
    try (MockedStatic<Runtime> mock = Mockito.mockStatic(Runtime.class)) {
      Process processMock = mock(Process.class);
      when(processMock.getInputStream()).thenReturn(new ByteArrayInputStream(new byte[0]));
      when(processMock.getErrorStream()).thenReturn(new ByteArrayInputStream(new byte[0]));
      when(processMock.getOutputStream()).thenReturn(mock(OutputStream.class));
      when(processMock.waitFor()).thenReturn(-1);
      Runtime runtimeMock = mock(Runtime.class);
      mock.when(Runtime::getRuntime).thenReturn(runtimeMock);
      when(runtimeMock.exec(ArgumentMatchers.anyString())).thenReturn(processMock);
      int pid = 1234;
      assertThrows(IOException.class, () ->
          Kill.kill(pid));
      verify(runtimeMock).exec(
          String.format("kill -%d %d", Kill.Signal.SIGTERM.getSigNum(), pid));
    }
  }

  @Test
  public void testSigkill() throws Exception {
    try (MockedStatic<Runtime> mock = Mockito.mockStatic(Runtime.class)) {
      Process processMock = mock(Process.class);
      when(processMock.getInputStream()).thenReturn(new ByteArrayInputStream(new byte[0]));
      when(processMock.getErrorStream()).thenReturn(new ByteArrayInputStream(new byte[0]));
      when(processMock.getOutputStream()).thenReturn(mock(OutputStream.class));
      when(processMock.exitValue()).thenReturn(0);
      Runtime runtimeMock = mock(Runtime.class);
      mock.when(Runtime::getRuntime).thenReturn(runtimeMock);
      when(runtimeMock.exec(ArgumentMatchers.anyString())).thenReturn(processMock);
      int pid = 1234;
      Kill.kill9(pid);
      verify(runtimeMock).exec(
          String.format("kill -%d %d", Kill.Signal.SIGKILL.getSigNum(), pid));
    }
  }

  @Test
  public void testStreamCloseExceptions() throws Exception {
    try (MockedStatic<Runtime> mock = Mockito.mockStatic(Runtime.class)) {
      Process processMock = mock(Process.class);
      InputStream stdoutIs = Mockito.spy(new ByteArrayInputStream(new byte[0]));
      InputStream stderrIs = Mockito.spy(new ByteArrayInputStream(new byte[0]));
      OutputStream os = Mockito.mock(OutputStream.class);
      when(processMock.getInputStream()).thenReturn(stdoutIs);
      when(processMock.getErrorStream()).thenReturn(stderrIs);
      when(processMock.getOutputStream()).thenReturn(os);
      when(processMock.exitValue()).thenReturn(0);
      doThrow(new IOException("stdout")).when(stdoutIs).close();
      doThrow(new IOException("stderr")).when(stderrIs).close();
      doThrow(new IOException("stdin")).when(os).close();
      Runtime runtimeMock = mock(Runtime.class);
      mock.when(Runtime::getRuntime).thenReturn(runtimeMock);
      when(runtimeMock.exec(ArgumentMatchers.anyString())).thenReturn(processMock);
      int pid = 1234;
      Kill.kill(pid);
      verify(runtimeMock).exec(
          String.format("kill -%d %d", Kill.Signal.SIGTERM.getSigNum(), pid));
    }
    mLogger.wasLogged("Failed to properly close stdout");
    mLogger.wasLogged("Failed to properly close stderr");
    mLogger.wasLogged("Failed to properly close stdin");
  }
}
