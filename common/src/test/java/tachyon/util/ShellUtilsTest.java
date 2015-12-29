/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.util;

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
    String testString = "tachyon";
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
