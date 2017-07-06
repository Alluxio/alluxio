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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
   * See https://paulgorman.org/technical/presentations/linux_username_conventions.pdf
   * in which the author refers to IEEE Std 1003.1-2001 regarding the standards for
   * valid POSIX usernames.
   */
  private static final Pattern USER_GROUP_PATTERN =
      Pattern.compile("(?<user>[\\w][\\w-]*\\$?)(:(?<group>[\\w][\\w-]*\\$?))?");

  /**
   * Changes the owner for the path specified in args.
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

  /**
   * Changes the owner and group for the path specified in args.
   *
   * @param path the {@link AlluxioURI} path to update
   * @param owner the new owner
   * @param group the new group
   * @param recursive whether to change the owner and group recursively
   */
  private void chown(AlluxioURI path, String owner, String group, boolean recursive)
      throws AlluxioException, IOException {
    SetAttributeOptions options =
        SetAttributeOptions.defaults().setOwner(owner).setGroup(group).setRecursive(recursive);
    mFileSystem.setAttribute(path, options);
    System.out.println("Changed owner:group of " + path + " to " + owner + ":" + group + ".");
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[1]);
    Matcher matchUserGroup = USER_GROUP_PATTERN.matcher(args[0]);
    if (matchUserGroup.matches()) {
      String owner = matchUserGroup.group("user");
      String group = matchUserGroup.group("group");
      if (group == null) {
        chown(path, owner, cl.hasOption("R"));
      } else {
        chown(path, owner, group, cl.hasOption("R"));
      }
      return 0;
    }
    System.out.println("Failed to parse " + args[0] + " as user or user:group");
    return -1;
  }

  @Override
  public String getUsage() {
    return "chown [-R] <owner>[:<group>] <path>";
  }

  @Override
  public String getDescription() {
    return "Changes the owner of a file or directory specified by args."
        + " Specify -R to change the owner recursively.";
  }
}
