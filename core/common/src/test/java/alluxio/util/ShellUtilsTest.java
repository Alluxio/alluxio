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

package alluxio.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link ShellUtils} class.
 */
public class ShellUtilsTest {

  /**
   * Tests the {@link ShellUtils#execCommand(String...)} method.
   *
   * @throws Throwable when the execution of the command fails
   */
  @Test
  public void execCommandTest() throws Throwable {
    String testString = "alluxio";
    // Execute echo for testing command execution.
    String result = ShellUtils.execCommand(new String[] {"bash", "-c", "echo " + testString});
    Assert.assertEquals(testString + "\n", result);
  }

  /**
   * Tests the {@link ShellUtils#execCommand(String...)} method for a group of commands.
   *
   * @throws Throwable when the execution of the commands fails
   */
  @Test
  public void execGetGroupCommandTest() throws Throwable {
    String result = ShellUtils.execCommand(ShellUtils.getGroupsForUserCommand("root"));
    // On Linux user "root" will be a part of the group "root". On OSX it will be a part of "admin".
    Assert.assertTrue(result.contains("root") || result.contains("admin"));
  }
}
