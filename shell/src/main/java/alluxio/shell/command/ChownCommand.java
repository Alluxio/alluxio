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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Changes the owner of a file or directory specified by args.
 */
@ThreadSafe
public final class ChownCommand extends AbstractShellCommand {

  private static final Option RECURSIVE_OPTION =
      Option.builder("R")
          .required(false)
          .hasArg(false)
          .desc("change owner recursively")
          .build();

  /**
   * Creates a new instance of {@link ChownCommand}.
   *
   * @param fs an Alluxio file system handle
   */
  public ChownCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "chown";
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
   * Changes the owner for the directory or file with the path specified in args.
   *
   * @param path The {@link AlluxioURI} path as the input of the command
   * @param owner The owner to be updated to the file or directory
   * @param recursive Whether change the owner recursively
   */
  private void chown(AlluxioURI path, String owner, boolean recursive)
      throws AlluxioException, IOException {
    SetAttributeOptions options =
        SetAttributeOptions.defaults().setOwner(owner).setRecursive(recursive);
    mFileSystem.setAttribute(path, options);
    System.out.println("Changed owner of " + path + " to " + owner);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    String owner = args[0];
    AlluxioURI path = new AlluxioURI(args[1]);
    chown(path, owner, cl.hasOption("R"));
    return 0;
  }

  @Override
  public String getUsage() {
    return "chown [-R] <owner> <path>";
  }

  @Override
  public String getDescription() {
    return "Changes the owner of a file or directory specified by args."
        + " Specify -R to change the owner recursively.";
  }
}
