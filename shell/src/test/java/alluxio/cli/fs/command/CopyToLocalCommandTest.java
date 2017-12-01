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

package alluxio.cli.fs.command;

import alluxio.exception.AlluxioException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

/**
 * A unit test.
 */
public class CopyToLocalCommandTest {
  private ByteArrayOutputStream mOutput = new ByteArrayOutputStream();
  private ByteArrayOutputStream mError = new ByteArrayOutputStream();

 @Before
  public void setupStreams() {
    System.setOut(new PrintStream(mOutput));
    System.setErr(new PrintStream(mError));
  }

  @After
  public void cleanupStreams() {
    System.setOut(null);
    System.setErr(null);
  }

  @Test
  public void testgetCommandName() throws AlluxioException, IOException {
    CopyToLocalCommand command = new CopyToLocalCommand(null);
    String ret = command.getCommandName();
    String expectedRet = "copyToLocal";
    Assert.assertEquals(ret, expectedRet);
  }

  @Test
  public void testgetNumOfArgs() throws AlluxioException, IOException {
    CopyToLocalCommand command = new CopyToLocalCommand(null);
    int ret = command.getNumOfArgs();
    Assert.assertEquals(ret, 2);
  }

  @Test
  public void testgetDescription() throws AlluxioException, IOException {
    CopyToLocalCommand command = new CopyToLocalCommand(null);
    String expectedRet = "Copies a file or a directory "
        + "from local filesystem to Alluxio filesystem.";
    String ret = command.getDescription();
    Assert.assertEquals(ret, expectedRet);
  }
}

