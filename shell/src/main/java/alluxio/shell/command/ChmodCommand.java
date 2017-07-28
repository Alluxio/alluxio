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

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.exception.AlluxioException;
import alluxio.security.authorization.Mode;
import alluxio.security.authorization.ModeParser;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Changes the permission of a file or directory specified by args.
 */
@ThreadSafe
public final class ChmodCommand extends AbstractShellCommand {

  private static final Option RECURSIVE_OPTION =
      Option.builder("R")
          .required(false)
          .hasArg(false)
          .desc("change mode recursively")
          .build();

  private final ModeParser mParser = new ModeParser();

  /**
   * Creates a new instance of {@link ChmodCommand}.
   *
   * @param fs an Alluxio file system handle
   */
  public ChmodCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "chmod";
  }

  @Override
  protected int getNumOfArgs() {
    return 2;
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(RECURSIVE_OPTION);
  }

  /**
   * Changes the permissions of directory or file with the path specified in args.
   *
   * @param path The {@link AlluxioURI} path as the input of the command
   * @param modeStr The new permission to be updated to the file or directory
   * @param recursive Whether change the permission recursively
   */
  private void chmod(AlluxioURI path, String modeStr, boolean recursive) throws
      AlluxioException, IOException {
    Mode mode = mParser.parse(modeStr);
    SetAttributeOptions options =
        SetAttributeOptions.defaults().setMode(mode).setRecursive(recursive);
    mFileSystem.setAttribute(path, options);
    System.out
        .println("Changed permission of " + path + " to " + Integer.toOctalString(mode.toShort()));
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    String modeStr = args[0];
    AlluxioURI path = new AlluxioURI(args[1]);
    chmod(path, modeStr, cl.hasOption("R"));
    return 0;
  }

  @Override
  public String getUsage() {
    return "chmod [-R] <mode> <path>";
  }

  @Override
  public String getDescription() {
    return "Changes the permission of a file or directory specified by args."
        + " Specify -R to change the permission recursively.";
  }
}
