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

package alluxio.client.cli.fs.command;

import alluxio.exception.ExceptionMessage;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Test for location command.
 */
public final class LocationCommandIntegrationTest extends AbstractFileSystemShellTest {
  @Test
  public void locationNotExist() throws IOException {
    int ret = sFsShell.run("location", "/NotExistFile");
    Assert.assertEquals(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/NotExistFile") + "\n",
        mOutput.toString());
    Assert.assertEquals(-1, ret);
  }
}
