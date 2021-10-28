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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MacOsProcessTableTest {

  private MacOsProcessTable mTable;

  @Before
  public void before() {
    mTable = Mockito.spy(new MacOsProcessTable());
  }

  @Test
  public void testIsProcessAliveFalse() throws IOException {
    doReturn(Collections.emptyMap()).when(mTable).getPsOutput();
    assertFalse(mTable.isProcessAlive(9999));
  }

  @Test
  public void testIsProcessAliveException() throws IOException {
    doThrow(new IOException("test exception")).when(mTable).getPsOutput();
    assertFalse(mTable.isProcessAlive(9999));
  }

  @Test
  public void testIsProcessAliveTrue() throws IOException {
    doReturn(ImmutableMap.of(9999, "java test.process.MainClass")).when(mTable).getPsOutput();
    assertTrue(mTable.isProcessAlive(9999));
  }

  @Test
  public void testGetAllPids() throws IOException {
    doReturn(ImmutableMap.of(9999, "java test.process.MainClass", 1234, "aaa", 555, "bbb"))
        .when(mTable).getPsOutput();
    assertEquals(3, mTable.getAllPids().count());
    Set<Integer> pids = mTable.getAllPids().collect(Collectors.toSet());
    assertTrue(pids.contains(9999));
    assertTrue(pids.contains(1234));
    assertTrue(pids.contains(555));
  }

  @Test
  public void testGetJavaPids() throws IOException {
    doReturn(ImmutableMap.of(9999, "java test.process.MainClass", 1234, "aaa", 555, "bbb"))
        .when(mTable).getPsOutput();
    assertEquals(3, mTable.getAllPids().count());
    Set<Integer> pids = mTable.getJavaPids().collect(Collectors.toSet());
    assertEquals(1, pids.size());
    assertTrue(pids.contains(9999));
  }

  @Test
  public void testGetJavaPid() throws IOException {
    doReturn(ImmutableMap.of(9999, "java " + getClass(), 1234, "aaa", 555, "bbb"))
        .when(mTable).getPsOutput();
    List<Integer> result = mTable.getJavaPid(null, getClass());
    assertEquals(1, result.size());
    assertEquals(9999, (int) result.get(0));
    result = mTable.getJavaPid(null, LinuxProcessTable.class);
    assertEquals(0, result.size());
  }

  @Test
  public void testGetJavaPidWithLimitedList() throws IOException {
    doReturn(ImmutableMap.of(9999, "java " + getClass(), 1234, "aaa", 555, "bbb"))
        .when(mTable).getPsOutput();
    List<Integer> result = mTable.getJavaPid(Collections.singleton(12345).stream(),
        getClass());
    assertEquals(0, result.size());
    result = mTable.getJavaPid(Collections.singleton(9999).stream(), getClass());
    assertEquals(1, result.size());
  }

  @Test
  public void testGetOsProcessError() throws Exception {
    String output = "PID COMMAND\n1 init\n2 java\n3 java alluxio.master.AlluxioMaster";
    MacOsProcessTable table = new MacOsProcessTable();
    try (MockedStatic<Runtime> runtimeMock = Mockito.mockStatic(Runtime.class)) {
      Process mockedProcess = mock(Process.class);
      Runtime mockedRuntime = mock(Runtime.class);
      when(mockedRuntime.exec(ArgumentMatchers.anyString()))
          .thenReturn(mockedProcess);
      runtimeMock.when(Runtime::getRuntime).thenReturn(mockedRuntime);
      InputStream stdoutIs = Mockito.spy(new ByteArrayInputStream(output.getBytes()));
      InputStream stderrIs = Mockito.spy(new ByteArrayInputStream(new byte[0]));
      OutputStream os = Mockito.mock(OutputStream.class);
      when(mockedProcess.getInputStream()).thenReturn(stdoutIs);
      when(mockedProcess.getErrorStream()).thenReturn(stderrIs);
      when(mockedProcess.getOutputStream()).thenReturn(os);
      doThrow(new IOException("stdout")).when(stdoutIs).close();
      doThrow(new IOException("stderr")).when(stderrIs).close();
      doThrow(new IOException("stdin")).when(os).close();
      doReturn(1).when(mockedProcess).waitFor();
      assertThrows(IOException.class, table::getPsOutput);
      verify(mockedProcess).getInputStream();
      verify(mockedProcess).getErrorStream();
      verify(stdoutIs).close();
      verify(stderrIs).close();
      verify(os).close();
    }
  }

  @Test
  public void testGetOsProcesses() throws IOException {
    String output = "PID COMMAND\n1 init\n2 java\n3 java alluxio.master.AlluxioMaster";
    MacOsProcessTable table = new MacOsProcessTable();
    try (MockedStatic<Runtime> runtimeMock = Mockito.mockStatic(Runtime.class)) {
      Process mockedProcess = mock(Process.class);
      Runtime mockedRuntime = mock(Runtime.class);
      when(mockedRuntime.exec(ArgumentMatchers.anyString()))
          .thenReturn(mockedProcess);
      runtimeMock.when(Runtime::getRuntime).thenReturn(mockedRuntime);
      InputStream stdoutIs = Mockito.spy(new ByteArrayInputStream(output.getBytes()));
      InputStream stderrIs = Mockito.spy(new ByteArrayInputStream(new byte[0]));
      OutputStream os = Mockito.mock(OutputStream.class);
      when(mockedProcess.getInputStream()).thenReturn(stdoutIs);
      when(mockedProcess.getErrorStream()).thenReturn(stderrIs);
      when(mockedProcess.getOutputStream()).thenReturn(os);
      when(mockedProcess.exitValue()).thenReturn(0);
      Map<Integer, String> cmds = table.getPsOutput();
      assertTrue(cmds.containsKey(1));
      assertTrue(cmds.containsKey(2));
      assertTrue(cmds.containsKey(3));
      assertEquals("java", cmds.get(2));
      assertEquals("java alluxio.master.AlluxioMaster", cmds.get(3));
      verify(mockedProcess).getInputStream();
      verify(mockedProcess).getErrorStream();
      verify(stdoutIs).close();
      verify(stderrIs).close();
      verify(os).close();
    }
  }

  @Test
  public void testInterruptedOnGetPs() throws IOException, InterruptedException {
    String output = "PID COMMAND\n1 init\n2 java\n3 java alluxio.master.AlluxioMaster";
    MacOsProcessTable table = new MacOsProcessTable();
    try (MockedStatic<Runtime> runtimeMock = Mockito.mockStatic(Runtime.class)) {
      Process mockedProcess = mock(Process.class);
      Runtime mockedRuntime = mock(Runtime.class);
      when(mockedRuntime.exec(ArgumentMatchers.anyString()))
          .thenReturn(mockedProcess);
      runtimeMock.when(Runtime::getRuntime).thenReturn(mockedRuntime);
      InputStream stdoutIs = Mockito.spy(new ByteArrayInputStream(output.getBytes()));
      InputStream stderrIs = Mockito.spy(new ByteArrayInputStream(new byte[0]));
      OutputStream os = Mockito.mock(OutputStream.class);
      when(mockedProcess.getInputStream()).thenReturn(stdoutIs);
      when(mockedProcess.getErrorStream()).thenReturn(stderrIs);
      when(mockedProcess.getOutputStream()).thenReturn(os);
      doThrow(new InterruptedException()).when(mockedProcess).waitFor();
      assertThrows(IOException.class, table::getPsOutput);
      assertTrue(Thread.currentThread().isInterrupted());
      verify(mockedProcess).getInputStream();
      verify(mockedProcess).getErrorStream();
      verify(stdoutIs).close();
      verify(stderrIs).close();
      verify(os).close();
    }
  }
}
