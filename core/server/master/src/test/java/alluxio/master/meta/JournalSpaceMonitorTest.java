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

package alluxio.master.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import alluxio.TestLoggerRule;
import alluxio.shell.CommandReturn;
import alluxio.util.OSUtils;
import alluxio.wire.JournalDiskInfo;

import org.apache.log4j.Level;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

public class JournalSpaceMonitorTest {

  private static final String CMD_RETURN_MOCK =
      "Filesystem     1024-blocks      Used Available Capacity Mounted on\n"
          + "/dev/nvme0n1p2   959863856 145802864 765232784      17% /";

  @Rule
  public TestLoggerRule mLogger = new TestLoggerRule();

  @BeforeClass
  public static void beforeClass() {
    Assume.assumeTrue(OSUtils.isLinux());
  }

  @Test
  public void testNonExistentJournalPath() {
    assertThrows(IllegalArgumentException.class,
        () -> new JournalSpaceMonitor("/nonexistent/path", 10));
  }

  @Test
  public void testSuccessfulInfo() throws IOException  {
    JournalSpaceMonitor monitor = Mockito.spy(
        new JournalSpaceMonitor(Paths.get(".").toAbsolutePath().toString(), 10));
    doReturn(new CommandReturn(0, CMD_RETURN_MOCK)).when(monitor).getRawDiskInfo();
    List<JournalDiskInfo> infos = monitor.getDiskInfo();
    assertEquals(1, infos.size());
    JournalDiskInfo info = infos.get(0);
    assertEquals("/dev/nvme0n1p2", info.getDiskPath());
    assertEquals(959863856L * 1024, info.getTotalAllocatedBytes());
    assertEquals(145802864L * 1024, info.getUsedBytes());
    assertEquals(765232784L * 1024, info.getAvailableBytes());
    assertEquals(83.0, Math.floor(info.getPercentAvailable()), 0.1);
  }

  @Test
  public void testFailedInfo() throws IOException  {
    JournalSpaceMonitor monitor = Mockito.spy(
        new JournalSpaceMonitor(Paths.get(".").toAbsolutePath().toString(), 10));
    doThrow(new IOException("couldnt run")).when(monitor).getRawDiskInfo();
    assertThrows(IOException.class, monitor::getDiskInfo);
  }

  @Test
  public void testLoggingPositive() throws IOException, InterruptedException {
    JournalSpaceMonitor monitor = Mockito.spy(
        new JournalSpaceMonitor(Paths.get(".").toAbsolutePath().toString(), 90));
    doReturn(new CommandReturn(0, CMD_RETURN_MOCK)).when(monitor).getRawDiskInfo();
    monitor.heartbeat();
    assertTrue(mLogger.wasLoggedWithLevel("The journal disk /dev/nvme0n1p2 backing the journal "
        + "has only .* space left", Level.WARN));
  }

  @Test
  public void testLoggingNegative() throws IOException, InterruptedException {
    JournalSpaceMonitor monitor = Mockito.spy(
        new JournalSpaceMonitor(Paths.get(".").toAbsolutePath().toString(), 10));
    doReturn(new CommandReturn(0, CMD_RETURN_MOCK)).when(monitor).getRawDiskInfo();
    monitor.heartbeat();
    assertFalse(mLogger.wasLoggedWithLevel("The journal disk /dev/nvme0n1p2 backing the journal "
        + "has only .* space left", Level.WARN));
  }
}
