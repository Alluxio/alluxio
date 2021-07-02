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

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.SetAttributePOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Changes the group of a file or directory specified by args.
 */
@ThreadSafe
@PublicApi
public final class ChgrpCommand extends AbstractFileSystemCommand {

  private static final Option RECURSIVE_OPTION =
      Option.builder("R")
          .required(false)
          .hasArg(false)
          .desc("change group recursively")
          .build();

  private String mGroup;

  /**
   * Creates a new instance of {@link ChgrpCommand}.
   *
   * @param fsContext an Alluxio file system handle
   */
  public ChgrpCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "chgrp";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 2);
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(RECURSIVE_OPTION);
  }

  /**
   * Changes the group for the directory or file with the path specified in args.
   *
   * @param path The {@link AlluxioURI} path as the input of the command
   * @param group The group to be updated to the file or directory
   * @param recursive Whether change the group recursively
   */
  private void chgrp(AlluxioURI path, String group, boolean recursive)
      throws AlluxioException, IOException {
    SetAttributePOptions options =
        SetAttributePOptions.newBuilder().setGroup(group).setRecursive(recursive).build();
    mFileSystem.setAttribute(path, options);
    System.out.println("Changed group of " + path + " to " + group);
  }

  @Override
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws IOException, AlluxioException {
    chgrp(path, mGroup, cl.hasOption("R"));
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    mGroup = args[0];
    AlluxioURI path = new AlluxioURI(args[1]);
    runWildCardCmd(path, cl);

    return 0;
  }

  @Override
  public String getUsage() {
    return "chgrp [-R] <group> <path>";
  }

  @Override
  public String getDescription() {
    return "Changes the group of a file or directory specified by args."
        + " Specify -R to change the group recursively.";
  }
}
