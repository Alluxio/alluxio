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
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import alluxio.Constants;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests the {@link ShellUtils} class.
 */
public final class ShellUtilsTest {

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
    Assert.assertEquals(testString + "\n", result);
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
    Assert.assertTrue(result.contains("root") || result.contains("admin"));
  }

  @Test
  public void parseRamfsMountInfoWithType() throws Exception {
    // Linux mount info.
    UnixMountInfo info =
        ShellUtils.parseMountInfo("ramfs on /mnt/ramdisk type ramfs (rw,relatime,size=1gb)");
    assertEquals("ramfs", info.getDeviceSpec());
    assertEquals("/mnt/ramdisk", info.getMountPoint());
    assertEquals("ramfs", info.getFsType());
    assertEquals(Long.valueOf(Constants.GB), info.getOptions().getSize());
  }

  @Test
  public void parseMountInfoWithoutType() throws Exception {
    // OS X mount info.
    UnixMountInfo info = ShellUtils.parseMountInfo("devfs on /dev (devfs, local, nobrowse)");
    assertEquals("devfs", info.getDeviceSpec());
    assertEquals("/dev", info.getMountPoint());
    assertEquals(null, info.getFsType());
    assertEquals(null, info.getOptions().getSize());
  }

  @Test
  public void parseTmpfsMountInfo() throws Exception {
    // Docker VM mount info.
    UnixMountInfo info = ShellUtils
        .parseMountInfo("shm on /dev/shm type tmpfs (rw,nosuid,nodev,noexec,relatime,size=65536k)");
    assertEquals("shm", info.getDeviceSpec());
    assertEquals("/dev/shm", info.getMountPoint());
    assertEquals("tmpfs", info.getFsType());
    assertEquals(Long.valueOf(65536 * Constants.KB), info.getOptions().getSize());
  }

  @Test
  public void parseMountInfoSpaceInPath() throws Exception {
    UnixMountInfo info = ShellUtils.parseMountInfo("/dev/disk4s1 on /Volumes/Space Path "
        + "(hfs, local, nodev, nosuid, read-only, noowners, quarantine)");
    assertEquals("/dev/disk4s1", info.getDeviceSpec());
    assertEquals("/Volumes/Space Path", info.getMountPoint());
    assertEquals(null, info.getFsType());
    assertEquals(null, info.getOptions().getSize());
  }

  @Test
  public void getMountInfo() throws Exception {
    assumeTrue(OSUtils.isMacOS() || OSUtils.isLinux());
    List<UnixMountInfo> info = ShellUtils.getUnixMountInfo();
    assertTrue(info.size() > 0);
  }
}
