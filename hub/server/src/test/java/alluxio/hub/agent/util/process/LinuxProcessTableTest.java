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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;

import alluxio.util.OSUtils;

import org.hamcrest.Matchers;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class LinuxProcessTableTest {

  @Test
  public void testGetJavaPids() throws Exception {
    // This test must be running, so there should be at least one Java PID
    Assume.assumeTrue(OSUtils.isLinux());
    LinuxProcessTable procInfo = new LinuxProcessTable();
    assertThat(procInfo.getJavaPids().count(), Matchers.greaterThan(0L));
  }

  @Test
  public void testGetJavaPidNull() throws IOException {
    LinuxProcessTable procInfo = Mockito.spy(new LinuxProcessTable());
    Mockito.doAnswer((Answer<Stream<Integer>>) (invocation) -> Stream.of(123)).when(procInfo)
            .getJavaPids();
    try (MockedStatic<Files> fileMock = Mockito.mockStatic(Files.class)) {
      fileMock.when(() -> Files.exists(ArgumentMatchers.any())).thenReturn(true);
      fileMock.when(() -> Files.readAllBytes(ArgumentMatchers.any()))
          .thenReturn(NodeStatusTest.class.getCanonicalName().getBytes());
      assertEquals(1, procInfo.getJavaPid(null, NodeStatusTest.class).size());
      assertEquals(123, (int) procInfo.getJavaPid(null, NodeStatusTest.class)
              .get(0));
    }
  }

  @Test
  public void testGetJavaPidNotNull() throws IOException {
    LinuxProcessTable procInfo = Mockito.spy(new LinuxProcessTable());
    Mockito.doAnswer((Answer<Stream<Integer>>) (invocation) -> Stream.of(123)).when(procInfo)
            .getAllPids();
    try (MockedStatic<Files> fileMock = Mockito.mockStatic(Files.class)) {
      fileMock.when(() -> Files.exists(ArgumentMatchers.any())).thenReturn(true);
      fileMock.when(
          () -> Files.readAllBytes(ArgumentMatchers.eq(Paths.get("/proc", 123 + "",
                  "comm"))))
          .thenReturn("java".getBytes());
      fileMock.when(
          () -> Files.readAllBytes(ArgumentMatchers.eq(Paths.get("/proc", 123 + "",
                  "cmdline"))))
          .thenReturn(NodeStatusTest.class.getCanonicalName().getBytes());
      assertEquals(1, procInfo.getJavaPid(procInfo.getJavaPids(), NodeStatusTest.class)
              .size());
      assertEquals(123, (int) procInfo.getJavaPid(procInfo.getJavaPids(),
              NodeStatusTest.class).get(0));
    }
  }

  @Test
  public void testReadBytesError() throws IOException {
    LinuxProcessTable procInfo = Mockito.spy(new LinuxProcessTable());
    Mockito.doAnswer((Answer<Stream<Integer>>) (invocation) -> Stream.of(123)).when(procInfo)
            .getAllPids();
    try (MockedStatic<Files> fileMock = Mockito.mockStatic(Files.class)) {
      fileMock.when(() -> Files.exists(ArgumentMatchers.any())).thenReturn(true);
      fileMock.when(
          () -> Files.readAllBytes(ArgumentMatchers.eq(Paths.get("/proc", 123 + "",
                  "comm"))))
          .thenThrow(new IOException("expected"));
      assertEquals(0, procInfo.getJavaPids().count());
    }
  }

  @Test
  public void testGetJavaPidFileNotExists() throws IOException {
    LinuxProcessTable procInfo = Mockito.spy(new LinuxProcessTable());
    Mockito.doAnswer((Answer<Stream<Integer>>) (inv) -> Stream.of(1234)).when(procInfo)
            .getJavaPids();
    try (MockedStatic<Files> fileMock = Mockito.mockStatic(Files.class)) {
      fileMock.when(() -> Files.exists(ArgumentMatchers.any())).thenReturn(false);
      assertEquals(0, procInfo.getJavaPid(procInfo.getJavaPids(), NodeStatusTest.class)
              .size());
    }
  }

  @Test
  public void testGetJavaPidsFileNotExists() throws IOException {
    LinuxProcessTable procInfo = Mockito.spy(new LinuxProcessTable());
    Mockito.doAnswer((Answer<Stream<Integer>>) (inv) -> Stream.of(1234)).when(procInfo)
            .getAllPids();
    try (MockedStatic<Files> fileMock = Mockito.mockStatic(Files.class)) {
      fileMock.when(() -> Files.exists(ArgumentMatchers.any())).thenReturn(false);
      assertEquals(0, procInfo.getJavaPids().count());
    }
  }

  @Test
  public void testGetJavaPidReadBytesException() throws IOException {
    LinuxProcessTable procInfo = Mockito.spy(new LinuxProcessTable());
    Mockito.doAnswer((Answer<Stream<Integer>>) (inv) -> Stream.of(1234)).when(procInfo)
            .getJavaPids();
    try (MockedStatic<Files> fileMock = Mockito.mockStatic(Files.class)) {
      fileMock.when(() -> Files.exists(ArgumentMatchers.any())).thenReturn(true);
      fileMock.when(() -> Files.readAllBytes(ArgumentMatchers.any()))
          .thenThrow(new IOException("expected"));
      assertEquals(0, procInfo.getJavaPid(procInfo.getJavaPids(), NodeStatusTest.class)
              .size());
    }
  }

  @Test
  public void testAliveProcess() throws IOException {
    try (MockedStatic<Files> fileMock = Mockito.mockStatic(Files.class)) {
      fileMock.when(() -> Files.exists(Paths.get("/proc", Integer.toString(1234))))
          .thenReturn(true);
      LinuxProcessTable spf = new LinuxProcessTable();
      assertTrue(spf.isProcessAlive(1234));
      assertFalse(spf.isProcessDead(1234));
    }
  }

  @Test
  public void testDeadProcess() throws IOException {
    try (MockedStatic<Files> fileMock = Mockito.mockStatic(Files.class)) {
      fileMock.when(() -> Files.exists(Paths.get("/proc", Integer.toString(1234))))
          .thenReturn(false);
      LinuxProcessTable spf = new LinuxProcessTable();
      assertFalse(spf.isProcessAlive(1234));
      assertTrue(spf.isProcessDead(1234));
    }
  }

  @Test
  public void testStopProcess() throws Exception {
    try (MockedStatic<Files> fileMock = Mockito.mockStatic(Files.class)) {
      try (MockedStatic<Kill> killMock = Mockito.mockStatic(Kill.class)) {
        MockedStatic.Verification verif = () -> Files.exists(Paths.get("/proc", Integer
                .toString(1234)));
        fileMock.when(verif).thenReturn(true).thenReturn(true).thenReturn(false);
        LinuxProcessTable spf = new LinuxProcessTable();
        spf.stopProcess(1234, 10000);
        fileMock.verify(times(3), verif);
      }
    }
  }
}
