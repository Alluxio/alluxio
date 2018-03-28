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

import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The base class for all the {@link Command} classes.
 */
@ThreadSafe
public abstract class AbstractCommand implements Command {

  protected AbstractCommand() {}

//  @Override
//  public Options getOptions() {
//    return new Options();
//  }

  @Override
  public CommandLine parseAndValidateArgs(String... args) throws InvalidArgumentException {
    Options opts = getOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;

    try {
      cmd = parser.parse(opts, args);
    } catch (ParseException e) {
      throw new InvalidArgumentException(
          String.format("Failed to parse args for %s", getCommandName()), e);
    }

    if (!checkArgs(cmd.getArgs())) {
      throw new InvalidArgumentException(ExceptionMessage.INVALID_ARGS_NUM
          .getMessage(getCommandName(), args.length));
    }
    return cmd;
  }
}
