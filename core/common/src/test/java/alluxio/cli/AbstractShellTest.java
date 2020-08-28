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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;

import alluxio.ConfigurationTestUtils;
import alluxio.SystemOutRule;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.cli.CommandLine;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayOutputStream;
import java.util.Map;

/**
 * Unit tests for {@link AbstractShell}.
 */
public final class AbstractShellTest {

  private static final String SHELL_NAME = "TestShell";

  public ByteArrayOutputStream mOutput = new ByteArrayOutputStream();

  @Rule
  public SystemOutRule mSystemOutRule = new SystemOutRule(mOutput);

  @Rule
  public ExpectedException mExpectedException = ExpectedException.none();

  private final class TestShell extends AbstractShell {

    public TestShell() {
      super(ImmutableMap.<String, String[]>builder().put("cmdAlias", new String[] {"cmd", "-O"})
          .put("stableAlias", new String[]{"cmd", "-O"})
          .build(), ImmutableSet.<String>builder().add("cmdAlias").build(),
          ConfigurationTestUtils.defaults());
    }

    @Override
    protected String getShellName() {
      return SHELL_NAME;
    }

    @Override
    protected Map<String, Command> loadCommands() {
      final Command cmd = mock(Command.class);
      try {
        when(cmd.run(any(CommandLine.class))).thenReturn(0);
        when(cmd.parseAndValidateArgs(any(String[].class)))
            .thenReturn(mock(CommandLine.class));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ImmutableMap.<String, Command>builder().put("cmd", cmd).build();
    }
  }

  @Test
  public void commandExists() throws Exception {
    TestShell shell = new TestShell();
    assertEquals(0, shell.run("cmd"));
  }

  @Test
  public void stableAliasNoWarning() throws Exception {
    TestShell shell = new TestShell();
    assertEquals(0, shell.run("stableAlias"));
    assertFalse(mOutput.toString().contains("WARNING: stableAlias"));
  }

  @Test
  public void commandAliasExists() throws Exception {
    TestShell shell = new TestShell();
    assertEquals(0, shell.run("cmdAlias"));
    String warningMsg = "WARNING: cmdAlias is not a stable CLI command. It may be removed in the"
        + " future. Use with caution in scripts. You may use 'cmd -O' instead.";
    String output = mOutput.toString();
    assertTrue(String.format("Output should contain proper warning.\nActual:   %s\nExpected: %s",
        output, warningMsg), output.contains(warningMsg));
  }

  @Test
  public void commandDoesNotExist() throws Exception {
    TestShell shell = new TestShell();
    assertTrue(shell.run("cmdNotExist") < 0);
  }
}
