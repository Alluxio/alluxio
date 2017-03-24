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

package alluxio.shell.command;

import alluxio.client.file.FileSystem;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The base class for all the {@link ShellCommand} classes. It provides a default argument
 * validation method and a place to hold the {@link FileSystem} client.
 */
@ThreadSafe
public abstract class AbstractShellCommand implements ShellCommand {

  protected FileSystem mFileSystem;
  protected static final Option RECURSIVE_OPTION =
      Option.builder("R")
            .required(false)
            .hasArg(false)
            .desc("recursive")
            .build();
  protected static final Option READONLY_OPTION =
      Option.builder("readonly")
          .required(false)
          .hasArg(false)
          .desc("readonly")
          .build();
  protected static final Option MOUNT_SHARED_OPTION =
      Option.builder("shared")
          .required(false)
          .hasArg(false)
          .desc("shared")
          .build();

  // TODO(gpang): Investigate property=value style of cmdline options. They didn't seem to
  // support spaces in values.
  protected static final Option PROPERTY_FILE_OPTION =
      Option.builder("P")
          .required(false)
          .numberOfArgs(1)
          .desc("properties file name")
          .build();
  protected static final Option FORCE_OPTION =
      Option.builder("f")
          .required(false)
          .hasArg(false)
          .desc("force")
          .build();
  protected static final Option LIST_DIR_AS_FILE_OPTION =
      Option.builder("d")
          .required(false)
          .hasArg(false)
          .desc("list directories as plain files")
          .build();
  protected static final Option LIST_PINNED_FILES_OPTION =
          Option.builder("p")
                  .required(false)
                  .hasArg(false)
                  .desc("list all pinned files")
                  .build();
  protected static final Option FIX_INCONSISTENT_FILES =
      Option.builder("r")
          .required(false)
          .hasArg(false)
          .desc("repair inconsistent files")
          .build();
  protected static final Option LIST_RAW_SIZE_OPTION =
      Option.builder("raw")
          .required(false)
          .hasArg(false)
          .desc("print raw sizes.")
          .build();

  protected AbstractShellCommand(FileSystem fs) {
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
  protected abstract int getNumOfArgs();

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
