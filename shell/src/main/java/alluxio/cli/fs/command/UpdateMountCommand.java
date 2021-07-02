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
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.MountPOptions;

import com.google.common.collect.Maps;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.Properties;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Update options for an Alluxio mount point.
 */
@ThreadSafe
@PublicApi
public final class UpdateMountCommand extends AbstractFileSystemCommand {

  private static final Option READONLY_OPTION =
      Option.builder()
          .longOpt("readonly")
          .required(false)
          .hasArg(false)
          .desc("mount point is readonly in Alluxio")
          .build();
  private static final Option SHARED_OPTION =
      Option.builder()
          .longOpt("shared")
          .required(false)
          .hasArg(false)
          .desc("mount point is shared")
          .build();
  private static final Option OPTION_OPTION =
      Option.builder()
          .longOpt("option")
          .required(false)
          .hasArg(true)
          .numberOfArgs(2)
          .argName("key=value")
          .valueSeparator('=')
          .desc("options for this mount point. For security reasons, no options from existing mount"
              + " point will be inherited.")
          .build();

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public UpdateMountCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "updateMount";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(READONLY_OPTION).addOption(SHARED_OPTION)
        .addOption(OPTION_OPTION);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI alluxioPath = new AlluxioURI(args[0]);
    MountPOptions.Builder optionsBuilder = MountPOptions.newBuilder();

    if (cl.hasOption(READONLY_OPTION.getLongOpt())) {
      optionsBuilder.setReadOnly(true);
    }
    if (cl.hasOption(SHARED_OPTION.getLongOpt())) {
      optionsBuilder.setShared(true);
    }
    if (cl.hasOption(OPTION_OPTION.getLongOpt())) {
      Properties properties = cl.getOptionProperties(OPTION_OPTION.getLongOpt());
      optionsBuilder.putAllProperties(Maps.fromProperties(properties));
    }
    mFileSystem.updateMount(alluxioPath, optionsBuilder.build());
    System.out.println("Updated mount point options at " + alluxioPath);
    return 0;
  }

  @Override
  public String getUsage() {
    return "updateMount [--readonly] [--shared] [--option <key=val>] <alluxioPath>";
  }

  @Override
  public String getDescription() {
    return "Updates options for a mount point while keeping the Alluxio metadata under the path.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    if (cl.getArgs().length != 1) {
      throw new InvalidArgumentException("Command updateMount takes 1 argument, not " + cl
          .getArgs().length);
    }
  }
}
