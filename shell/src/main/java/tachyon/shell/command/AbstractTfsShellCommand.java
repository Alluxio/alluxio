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

package tachyon.shell.command;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import tachyon.client.file.FileSystem;
import tachyon.conf.TachyonConf;

/**
 * The base class for all the {@link TfsShellCommand} classes. It provides a default argument
 * validation method and a place to hold the {@link FileSystem} client.
 */
@ThreadSafe
public abstract class AbstractTfsShellCommand implements TfsShellCommand {

  protected TachyonConf mTachyonConf;
  protected FileSystem mFileSystem;

  protected AbstractTfsShellCommand(TachyonConf conf, FileSystem fs) {
    mTachyonConf = conf;
    mFileSystem = fs;
  }

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
  abstract int getNumOfArgs();

  /**
   * Gets the supported Options of the command.
   *
   * @return the Options
   */
  protected Options getOptions() {
    return new Options();
  }

  @Override
  public CommandLine parseAndValidateArgs(String... args) {
    Options opts = getOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;

    try {
      cmd = parser.parse(opts, args, true /* stopAtNonOption */);
    } catch (ParseException e) {
      // TODO(ifcharming): improve the error message when an unregistered option appears
      System.err.println("Unable to parse input args: " + e.getMessage());
      return null;
    }

    if (!validateArgs(cmd.getArgs())) {
      return null;
    }
    return cmd;
  }
}
