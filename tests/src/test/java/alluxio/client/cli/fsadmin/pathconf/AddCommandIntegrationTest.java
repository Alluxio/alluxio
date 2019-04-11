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

package alluxio.client.cli.fsadmin.pathconf;

import alluxio.cli.fsadmin.FileSystemAdminShell;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.cli.fs.AbstractShellIntegrationTest;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for pathConf add command.
 */
public class AddCommandIntegrationTest extends AbstractShellIntegrationTest {
  private static final String DIR1 = "/a/b/";
  private static final PropertyKey PROPERTY_KEY11 = PropertyKey.USER_FILE_READ_TYPE_DEFAULT;
  private static final String PROPERTY_VALUE11 = ReadType.NO_CACHE.toString();
  private static final PropertyKey PROPERTY_KEY12 = PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT;
  private static final String PROPERTY_VALUE12 = WriteType.MUST_CACHE.toString();
  private static final String DIR2 = "/a/b/c/";
  private static final PropertyKey PROPERTY_KEY2 = PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT;
  private static final String PROPERTY_VALUE2 = WriteType.THROUGH.toString();

  private String format(PropertyKey key, String value) {
    return key.getName() + "=" + value;
  }

  @Test
  public void add() throws Exception {
    try (FileSystemAdminShell shell = new FileSystemAdminShell(ServerConfiguration.global())) {
      int ret = shell.run("pathConf", "list");
      Assert.assertEquals(0, ret);
      String output = mOutput.toString();
      Assert.assertEquals("", output);

      mOutput.reset();
      ret = shell.run("pathConf", "add", "--property", format(PROPERTY_KEY11, PROPERTY_VALUE11),
          "--property", format(PROPERTY_KEY12, PROPERTY_VALUE12), DIR1);
      Assert.assertEquals(0, ret);
      output = mOutput.toString();
      Assert.assertEquals("", output);

      mOutput.reset();
      ret = shell.run("pathConf", "list");
      Assert.assertEquals(0, ret);
      output = mOutput.toString();
      Assert.assertEquals(DIR1 + "\n", output);

      mOutput.reset();
      ret = shell.run("pathConf", "add", "--property", format(PROPERTY_KEY2, PROPERTY_VALUE2),
        DIR2);
      Assert.assertEquals(0, ret);
      output = mOutput.toString();
      Assert.assertEquals("", output);

      mOutput.reset();
      ret = shell.run("pathConf", "list");
      Assert.assertEquals(0, ret);
      output = mOutput.toString();
      Assert.assertEquals(DIR1 + "\n" + DIR2 + "\n", output);

      mOutput.reset();
      ret = shell.run("pathConf", "show", DIR1);
      Assert.assertEquals(0, ret);
      String expected = format(PROPERTY_KEY11, PROPERTY_VALUE11) + "\n"
          + format(PROPERTY_KEY12, PROPERTY_VALUE12) + "\n";
      output = mOutput.toString();
      Assert.assertEquals(expected, output);

      mOutput.reset();
      ret = shell.run("pathConf", "show", DIR2);
      Assert.assertEquals(0, ret);
      expected = format(PROPERTY_KEY2, PROPERTY_VALUE2) + "\n";
      output = mOutput.toString();
      Assert.assertEquals(expected, output);
    }
  }
}
