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

package alluxio.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import alluxio.Constants;

import com.google.common.base.Optional;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

/**
 * Tests the {@link ShellUtils} class.
 */
public final class ShellUtilsTest {

  @Rule
  public ExpectedException mExceptionRule = ExpectedException.none();

  /**
   * Tests the {@link ShellUtils#execCommand(String...)} method.
   *
   * @throws Throwable when the execution of the command fails
   */
  @Test
  public void execCommand() throws Exception {
    String testString = "alluxio";
    // Execute echo for testing command execution.
    String result = ShellUtils.execCommand("bash", "-c", "echo " + testString);
    assertEquals(testString + "\n", result);
  }

  @Test
  public void execCommandFail() throws Exception {
    String testString = "false";
    mExceptionRule.expect(ShellUtils.ExitCodeException.class);
    // run a command that guarantees to fail
    String result = ShellUtils.execCommand("bash", "-c", " " + testString);
    assertEquals(testString + "\n", result);
  }

  /**
   * Tests the {@link ShellUtils#execCommand(String...)} method for a group of commands.
   *
   * @throws Throwable when the execution of the commands fails
   */
  @Test
  public void execGetGroupCommand() throws Exception {
    String result = ShellUtils.execCommand(ShellUtils.getGroupsForUserCommand("root"));
    // On Linux user "root" will be a part of the group "root". On OSX it will be a part of "admin".
    assertTrue(result.contains("root") || result.contains("admin"));
  }

  @Test
  public void parseRamfsMountInfoWithType() throws Exception {
    // Linux mount info.
    UnixMountInfo info =
        ShellUtils.parseMountInfo("ramfs on /mnt/ramdisk type ramfs (rw,relatime,size=1gb)");
    assertEquals(Optional.of("ramfs"), info.getDeviceSpec());
    assertEquals(Optional.of("/mnt/ramdisk"), info.getMountPoint());
    assertEquals(Optional.of("ramfs"), info.getFsType());
    assertEquals(Optional.of(Long.valueOf(Constants.GB)), info.getOptions().getSize());
  }

  @Test
  public void parseMountInfoWithoutType() throws Exception {
    // OS X mount info.
    UnixMountInfo info = ShellUtils.parseMountInfo("devfs on /dev (devfs, local, nobrowse)");
    assertEquals(Optional.of("devfs"), info.getDeviceSpec());
    assertEquals(Optional.of("/dev"), info.getMountPoint());
    assertFalse(info.getFsType().isPresent());
    assertFalse(info.getOptions().getSize().isPresent());
  }

  @Test
  public void parseTmpfsMountInfo() throws Exception {
    // Docker VM mount info.
    UnixMountInfo info = ShellUtils
        .parseMountInfo("shm on /dev/shm type tmpfs (rw,nosuid,nodev,noexec,relatime,size=65536k)");
    assertEquals(Optional.of("shm"), info.getDeviceSpec());
    assertEquals(Optional.of("/dev/shm"), info.getMountPoint());
    assertEquals(Optional.of("tmpfs"), info.getFsType());
    assertEquals(Optional.of(Long.valueOf(65536 * Constants.KB)), info.getOptions().getSize());
  }

  @Test
  public void parseMountInfoSpaceInPath() throws Exception {
    UnixMountInfo info = ShellUtils.parseMountInfo("/dev/disk4s1 on /Volumes/Space Path "
        + "(hfs, local, nodev, nosuid, read-only, noowners, quarantine)");
    assertEquals(Optional.of("/dev/disk4s1"), info.getDeviceSpec());
    assertEquals(Optional.of("/Volumes/Space Path"), info.getMountPoint());
    assertFalse(info.getFsType().isPresent());
    assertFalse(info.getOptions().getSize().isPresent());
  }

  @Test
  public void parseMountInfoInvalidOutput() throws Exception {
    UnixMountInfo info = ShellUtils.parseMountInfo("invalid output");
    assertFalse(info.getDeviceSpec().isPresent());
    assertFalse(info.getMountPoint().isPresent());
    assertFalse(info.getFsType().isPresent());
    assertFalse(info.getOptions().getSize().isPresent());
  }

  @Test
  public void getMountInfo() throws Exception {
    assumeTrue(OSUtils.isMacOS() || OSUtils.isLinux());
    List<UnixMountInfo> info = ShellUtils.getUnixMountInfo();
    assertTrue(info.size() > 0);
  }
}
