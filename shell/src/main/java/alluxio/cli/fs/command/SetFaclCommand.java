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
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.SetAclOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.security.authorization.AclEntry;
import alluxio.wire.SetAclAction;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Displays ACL info of a path.
 */
@ThreadSafe
public final class SetFaclCommand extends AbstractFileSystemCommand {
  private static final Option RECURSIVE_OPTION = Option.builder("R")
      .required(false)
      .hasArg(false)
      .desc("Apply to all files and directories recursively")
      .build();
  private static final Option SET_OPTION = Option.builder()
      .longOpt("set")
      .required(false)
      .hasArg()
      .desc("Fully replace the ACL while discarding existing entries. New ACL must be a comma "
          + "separated list of entries, and must include user, group, and other for "
          + "compatibility with permission bits.")
      .build();
  private static final Option MODIFY_OPTION = Option.builder("m")
      .required(false)
      .hasArg()
      .desc("Modify the ACL by adding/overwriting new entries.")
      .build();
  private static final Option REMOVE_OPTION = Option.builder("x")
      .required(false)
      .hasArg()
      .desc("Removes specified ACL entries.")
      .build();
  private static final Option REMOVE_ALL_OPTION = Option.builder("b")
      .required(false)
      .hasArg(false)
      .desc("Removes all of the ACL entries, except for the base entries.")
      .build();

  /**
   * @param fs the filesystem of Alluxio
   */
  public SetFaclCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "setfacl";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(RECURSIVE_OPTION).addOption(SET_OPTION).addOption(MODIFY_OPTION)
        .addOption(REMOVE_OPTION).addOption(REMOVE_ALL_OPTION);
  }

  @Override
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException {
    SetAclOptions options = SetAclOptions.defaults().setRecursive(false);
    if (cl.hasOption(RECURSIVE_OPTION.getOpt())) {
      options.setRecursive(true);
    }

    if (cl.hasOption(SET_OPTION.getLongOpt())) {
      String aclList = cl.getOptionValue(SET_OPTION.getLongOpt());
      List<AclEntry> entries = Arrays.stream(aclList.split(",")).map(AclEntry::fromCliString)
          .collect(Collectors.toList());
      mFileSystem.setAcl(path, SetAclAction.REPLACE, entries, options);
    } else if (cl.hasOption(MODIFY_OPTION.getOpt())) {
      String aclList = cl.getOptionValue(MODIFY_OPTION.getOpt());
      List<AclEntry> entries = Arrays.stream(aclList.split(",")).map(AclEntry::fromCliString)
          .collect(Collectors.toList());
      mFileSystem.setAcl(path, SetAclAction.MODIFY, entries, options);
    } else if (cl.hasOption(REMOVE_OPTION.getOpt())) {
      String aclList = cl.getOptionValue(REMOVE_OPTION.getOpt());
      List<AclEntry> entries =
          Arrays.stream(aclList.split(",")).map(AclEntry::fromCliStringWithoutPermissions)
              .collect(Collectors.toList());
      mFileSystem.setAcl(path, SetAclAction.REMOVE, entries, options);
    } else if (cl.hasOption(REMOVE_ALL_OPTION.getOpt())) {
      mFileSystem.setAcl(path, SetAclAction.REMOVE_ALL, Collections.emptyList(), options);
    } else {
      throw new IllegalArgumentException("nothing to execute.");
    }
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    runWildCardCmd(path, cl);

    return 0;
  }

  @Override
  public String getUsage() {
    return "setfacl [-R] [--set | -m | -x <acl_entries> <path>] | [-b | -k <path>]";
  }

  @Override
  public String getDescription() {
    return "Sets the access control list (ACL) for a path.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }
}
