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
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.MountOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.util.FormatUtils;
import alluxio.wire.MountPointInfo;

import com.google.common.collect.Maps;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Mounts a UFS path onto an Alluxio path.
 */
@ThreadSafe
public final class MountCommand extends AbstractFileSystemCommand {

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
          .desc("options associated with this mount point")
          .build();
  private static final String LEFT_ALIGN_FORMAT = "%-60s  on  %-20s (%s, capacity=%s,"
      + " used=%s, %sread-only, %sshared, ";

  /**
   * @param fs the filesystem of Alluxio
   */
  public MountCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "mount";
  }

  @Override
  protected int getNumOfArgs() {
    return 2;
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(READONLY_OPTION).addOption(SHARED_OPTION)
        .addOption(OPTION_OPTION);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    if (args.length == 0) {
      Map<String, MountPointInfo> mountTable = mFileSystem.getMountTable();
      printMountInfo(mountTable);
      return 0;
    }
    AlluxioURI alluxioPath = new AlluxioURI(args[0]);
    AlluxioURI ufsPath = new AlluxioURI(args[1]);
    MountOptions options = MountOptions.defaults();

    if (cl.hasOption(READONLY_OPTION.getLongOpt())) {
      options.setReadOnly(true);
    }
    if (cl.hasOption(SHARED_OPTION.getLongOpt())) {
      options.setShared(true);
    }
    if (cl.hasOption(OPTION_OPTION.getLongOpt())) {
      Properties properties = cl.getOptionProperties(OPTION_OPTION.getLongOpt());
      options.setProperties(Maps.fromProperties(properties));
    }
    mFileSystem.mount(alluxioPath, ufsPath, options);
    System.out.println("Mounted " + ufsPath + " at " + alluxioPath);
    return 0;
  }

  /**
   * Prints mount information for a mount table.
   *
   * @param mountTable the mount table to get information from
   */
  public static void printMountInfo(Map<String, MountPointInfo> mountTable) {
    for (Map.Entry<String, MountPointInfo> entry : mountTable.entrySet()) {
      String mMountPoint = entry.getKey();
      MountPointInfo mountPointInfo = entry.getValue();

      long capacityBytes = mountPointInfo.getUfsCapacityBytes();
      long usedBytes = mountPointInfo.getUfsUsedBytes();

      String usedPercentageInfo = "";
      if (capacityBytes > 0) {
        int usedPercentage = (int) (100L * usedBytes / capacityBytes);
        usedPercentageInfo = String.format("(%s%%)", usedPercentage);
      }

      System.out.format(LEFT_ALIGN_FORMAT, mountPointInfo.getUfsUri(), mMountPoint,
          mountPointInfo.getUfsType(), FormatUtils.getSizeFromBytes(capacityBytes),
          FormatUtils.getSizeFromBytes(usedBytes) + usedPercentageInfo,
          mountPointInfo.getReadOnly() ? "" : "not ",
          mountPointInfo.getShared() ? "" : "not ");
      System.out.println("properties=" + mountPointInfo.getProperties() + ")");
    }
  }

  @Override
  public String getUsage() {
    return "mount [--readonly] [--shared] [--option <key=val>] <alluxioPath> <ufsURI>";
  }

  @Override
  public String getDescription() {
    return "Mounts a UFS path onto an Alluxio path.";
  }

  @Override
  public void validateArgs(String... args) throws InvalidArgumentException {
    if (args.length != 2 && args.length != 0) {
      throw new InvalidArgumentException(
          ExceptionMessage.INVALID_ARGS_GENERIC.getMessage(getCommandName()));
    }
  }
}
