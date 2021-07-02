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
 * Changes the replication level of a file or directory specified by args.
 */
@ThreadSafe
@PublicApi
public final class SetReplicationCommand extends AbstractFileSystemCommand {

  private static final Option MAX_OPTION =
      Option.builder().longOpt("max").required(false).numberOfArgs(1)
          .desc("the maximum number of replicas")
          .build();
  private static final Option MIN_OPTION =
      Option.builder().longOpt("min").required(false).numberOfArgs(1)
          .desc("the minimum number of replicas")
          .build();
  private static final Option RECURSIVE_OPTION =
      Option.builder("R").required(false).hasArg(false).desc("set replication recursively")
          .build();

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public SetReplicationCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "setReplication";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(RECURSIVE_OPTION).addOption(MAX_OPTION)
        .addOption(MIN_OPTION);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }

  /**
   * Changes the replication level of directory or file with the path specified in args.
   *
   * @param path The {@link AlluxioURI} path as the input of the command
   * @param replicationMax the max replicas, null if not to set
   * @param replicationMin the min replicas, null if not to set
   * @param recursive Whether change the permission recursively
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException when non-Alluxio exception occurs
   */
  private void setReplication(AlluxioURI path, Integer replicationMax, Integer replicationMin,
      boolean recursive) throws AlluxioException, IOException {
    SetAttributePOptions.Builder optionsBuilder =
        SetAttributePOptions.newBuilder().setRecursive(recursive);
    String message = "Changed the replication level of " + path + "\n";
    if (replicationMax != null) {
      optionsBuilder.setReplicationMax(replicationMax);
      message += "replicationMax was set to " + replicationMax + "\n";
    }
    if (replicationMin != null) {
      optionsBuilder.setReplicationMin(replicationMin);
      message += "replicationMin was set to " + replicationMin + "\n";
    }
    mFileSystem.setAttribute(path, optionsBuilder.build());
    System.out.println(message);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    Integer replicationMax = cl.hasOption(MAX_OPTION.getLongOpt())
        ? Integer.valueOf(cl.getOptionValue(MAX_OPTION.getLongOpt())) : null;
    Integer replicationMin = cl.hasOption(MIN_OPTION.getLongOpt())
        ? Integer.valueOf(cl.getOptionValue(MIN_OPTION.getLongOpt())) : null;
    boolean recursive = cl.hasOption(RECURSIVE_OPTION.getOpt());
    if (replicationMax == null && replicationMin == null) {
      throw new IOException("At least one option of '--max' or '--min' must be specified");
    }
    if (replicationMax != null && replicationMin != null && replicationMax >= 0
        && replicationMax < replicationMin) {
      throw new IOException("Invalid values for '--max' and '--min' options");
    }
    setReplication(path, replicationMax, replicationMin, recursive);
    return 0;
  }

  @Override
  public String getUsage() {
    return "setReplication [-R] [--max <num> | --min <num>] <path>";
  }

  @Override
  public String getDescription() {
    return "Sets the minimum/maximum number of replicas for the file or directory at given path. "
        + "Specify '-1' as the argument of '--max' option to indicate no limit of the maximum "
        + "number of replicas. If 'path' is a directory and '-R' is specified, it will recursively "
        + "set all files in this directory.";
  }
}
