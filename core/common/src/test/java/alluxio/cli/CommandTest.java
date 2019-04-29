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

package alluxio.cli;

import static alluxio.exception.ExceptionMessage.INVALID_ARGS_NUM;

import static org.junit.Assert.assertEquals;

import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link Command}.
 */
public final class CommandTest {

  private static final String COMMAND_NAME = "TestCommand";

  @Rule
  public ExpectedException mExpectedException = ExpectedException.none();

  private final class TestCommand implements Command {

    public TestCommand() {
    }

    @Override
    public String getCommandName() {
      return COMMAND_NAME;
    }

    @Override
    public String getDescription() {
      return "Description";
    }

    @Override
    public void validateArgs(CommandLine cl) throws InvalidArgumentException {
      CommandUtils.checkNumOfArgsEquals(this, cl, 1);
    }

    @Override
    public String getUsage() {
      return "usage";
    }

    @Override
    public int run(CommandLine cl) {
      return 0;
    }
  }

  @Test
  public void expectedNumArgs() throws Exception {
    TestCommand cmd = new TestCommand();
    CommandLine commandLine = cmd.parseAndValidateArgs("arg1");
    assertEquals(1, commandLine.getArgs().length);
  }

  @Test
  public void unexpectedNumArgs() throws Exception {
    TestCommand cmd = new TestCommand();

    mExpectedException.expect(InvalidArgumentException.class);
    mExpectedException.expectMessage(INVALID_ARGS_NUM.getMessage(cmd.getCommandName(), 1, 2));
    cmd.parseAndValidateArgs("arg1", "arg2");
  }
}
