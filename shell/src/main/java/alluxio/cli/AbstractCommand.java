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

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * The base class for all the {@link Command} classes.
 */
@ThreadSafe
public abstract class AbstractCommand implements Command {

  protected AbstractCommand() {}

  /**
   * Checks if the arguments are valid.
   *
   * @param args the arguments for the command, excluding the command name and options
   * @return whether the args are valid
   */
  protected boolean validateArgs(String... args) {
    boolean valid = args.length == getNumOfArgs();
    if (!valid) {
      System.out.println(getCommandName() + " takes " + getNumOfArgs() + " arguments, " + " not "
          + args.length + "\n");
    }
    return valid;
  }

  /**
   * Gets the expected number of arguments of the command.
   *
   * @return the number of arguments
   */
  protected abstract int getNumOfArgs();

  @Override
  public Options getOptions() {
    return new Options();
  }

  @Override
  public CommandLine parseAndValidateArgs(String... args) {
    Options opts = getOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;

    try {
      cmd = parser.parse(opts, args);
    } catch (ParseException e) {
      System.err.println(String.format("%s: %s", getCommandName(), e.getMessage()));
      return null;
    }

    if (!validateArgs(cmd.getArgs())) {
      return null;
    }
    return cmd;
  }
}