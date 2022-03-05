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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.TestLoggerRule;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.hub.proto.AlluxioNodeType;
import alluxio.hub.test.BaseHubTest;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class ProcessLauncherTest extends BaseHubTest {

  @Rule
  public TestLoggerRule mLogger = new TestLoggerRule();

  @Test
  public void scriptNotExists() throws Exception {
    InstancedConfiguration conf = getTestConfig();
    conf.set(PropertyKey.HOME, "/path/to/non/existent/directory");
    assertThrows(IOException.class, () -> new ProcessLauncher(conf));
  }

  @Test
  public void testCreateProc() throws Exception {
    Process mockProcess = mock(Process.class);
    when(mockProcess.getOutputStream())
        .thenAnswer((inv) -> new ByteArrayOutputStream(1));
    when(mockProcess.getInputStream())
        .thenAnswer((inv) -> new ByteArrayInputStream(new byte[1]));
    when(mockProcess.getErrorStream())
        .thenAnswer((inv) -> new ByteArrayInputStream(new byte[1]));
    doReturn(false).doReturn(true)
        .when(mockProcess).waitFor(500, TimeUnit.MILLISECONDS);
    testCreateProcwithMock(mockProcess);
  }

  @Test
  public void testCreateProcStreamErrors() throws Exception {
    Process mockProcess = mock(Process.class);
    InputStream stdoutIs = Mockito.spy(new ByteArrayInputStream(new byte[0]));
    InputStream stderrIs = Mockito.spy(new ByteArrayInputStream(new byte[0]));
    OutputStream os = Mockito.mock(OutputStream.class);
    when(mockProcess.getInputStream()).thenReturn(stdoutIs);
    when(mockProcess.getErrorStream()).thenReturn(stderrIs);
    when(mockProcess.getOutputStream()).thenReturn(os);
    doThrow(new IOException("stdout")).when(stdoutIs).close();
    doThrow(new IOException("stderr")).when(stderrIs).close();
    doThrow(new IOException("stdin")).when(os).close();
    doReturn(false).doReturn(true)
        .when(mockProcess).waitFor(500, TimeUnit.MILLISECONDS);
    testCreateProcwithMock(mockProcess);
    mLogger.wasLogged("Failed to close stdin");
    mLogger.wasLogged("Failed to close stdout/stderr");
  }

  void testCreateProcwithMock(Process mockedProcess) throws Exception {
    InstancedConfiguration conf = getTestConfig();
    conf.set(PropertyKey.HOME, "/path/to/non/existent/directory");
    try (MockedStatic<Files> files = Mockito.mockStatic(Files.class)) {
      files.when(() -> Files.exists(ArgumentMatchers.any())).thenReturn(true);
      ProcessLauncher l = new ProcessLauncher(getTestConfig());
      try (MockedStatic<Runtime> runtimeMock = Mockito.mockStatic(Runtime.class)) {
        Runtime mockedRuntime = mock(Runtime.class);
        runtimeMock.when(Runtime::getRuntime).thenReturn(mockedRuntime);
        for (AlluxioNodeType t : AlluxioNodeType.values()) {
          doReturn(mockedProcess).when(mockedRuntime).exec(ArgumentMatchers.any(String.class));
          l.start(t);
          verify(mockedRuntime).exec(
              ArgumentMatchers.contains("alluxio-start.sh " + t.toString().toLowerCase()));
        }
      }
    }
  }

  @Test
  public void testStop() throws Exception {
    try (MockedStatic<Files> fileMock = Mockito.mockStatic(Files.class)) {
      fileMock.when(() -> Files.exists(ArgumentMatchers.any())).thenReturn(true);
      try (MockedStatic<ProcessTable.Factory> mock = Mockito
              .mockStatic(ProcessTable.Factory.class)) {
        ProcessTable mockPs = Mockito.mock(ProcessTable.class);
        mock.when(ProcessTable.Factory::getInstance).thenReturn(mockPs);
        when(mockPs.getJavaPid(ArgumentMatchers.any(), ArgumentMatchers.any()))
            .thenReturn(Collections.singletonList(1234));
        ProcessLauncher pl = new ProcessLauncher(getTestConfig());
        pl.stop(AlluxioNodeType.MASTER);
        verify(mockPs).getJavaPid(ArgumentMatchers.any(), ArgumentMatchers.any());
        verify(mockPs).stopProcess(1234, 30000);
      }
    }
  }

  @Test
  public void testStopNoPid() throws Exception {
    try (MockedStatic<Files> fileMock = Mockito.mockStatic(Files.class)) {
      fileMock.when(() -> Files.exists(ArgumentMatchers.any())).thenReturn(true);
      try (MockedStatic<ProcessTable.Factory> mock =
          Mockito.mockStatic(ProcessTable.Factory.class)) {
        ProcessTable mockPs = Mockito.mock(ProcessTable.class);
        mock.when(ProcessTable.Factory::getInstance).thenReturn(mockPs);
        when(mockPs.getJavaPid(ArgumentMatchers.any(), ArgumentMatchers.any()))
            .thenReturn(Collections.singletonList(-1));
        ProcessLauncher pl = new ProcessLauncher(getTestConfig());
        pl.stop(AlluxioNodeType.MASTER);
        verify(mockPs).getJavaPid(ArgumentMatchers.any(), ArgumentMatchers.any());
      }
    }
  }
}
