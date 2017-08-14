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

import alluxio.cli.command.AbstractCommand;
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
public abstract class AbstractShellCommand extends AbstractCommand implements ShellCommand {

  protected FileSystem mFileSystem;

  protected static final String REMOVE_UNCHECKED_OPTION_CHAR = "U";
  protected static final Option REMOVE_UNCHECKED_OPTION =
      Option.builder(REMOVE_UNCHECKED_OPTION_CHAR)
            .required(false)
            .hasArg(false)
            .desc("remove directories without checking UFS contents are in sync")
            .build();

  protected AbstractShellCommand(FileSystem fs) {
    mFileSystem = fs;
  }
}
