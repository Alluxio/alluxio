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
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.exception.AlluxioException;
import alluxio.util.FormatUtils;
import alluxio.util.SecurityUtils;
import alluxio.wire.LoadMetadataType;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Displays information for all directories and files directly under the path specified in args.
 */
@ThreadSafe
public final class LsCommand extends WithWildCardPathCommand {
  public static final String STATE_FOLDER = "Directory";
  public static final String STATE_FILE_IN_MEMORY = "In Memory";
  public static final String STATE_FILE_NOT_IN_MEMORY = "Not In Memory";

  /**
   * Formats the ls result string.
   *
   * @param acl whether security is enabled
   * @param isFolder whether this path is a file or a folder
   * @param permission permission string
   * @param userName user name
   * @param groupName group name
   * @param size size of the file in bytes
   * @param createTimeMs the epoch time in ms when the path is created
   * @param inMemory whether the file is in memory
   * @param path path of the file or folder
   * @return the formatted string according to acl and isFolder
   */
  public static String formatLsString(boolean acl, boolean isFolder, String permission,
      String userName, String groupName, long size, long createTimeMs, boolean inMemory,
      String path) {
    String memoryState;
    if (isFolder) {
      memoryState = STATE_FOLDER;
    } else {
      memoryState = inMemory ? STATE_FILE_IN_MEMORY : STATE_FILE_NOT_IN_MEMORY;
    }
    if (acl) {
      return String.format(Constants.LS_FORMAT, permission, userName, groupName,
          FormatUtils.getSizeFromBytes(size), CommandUtils.convertMsToDate(createTimeMs),
          memoryState, path);
    } else {
      return String.format(Constants.LS_FORMAT_NO_ACL, FormatUtils.getSizeFromBytes(size),
          CommandUtils.convertMsToDate(createTimeMs), memoryState, path);
    }
  }

  /**
   * Constructs a new instance to display information for all directories and files directly under
   * the path specified in args.
   *
   * @param fs the filesystem of Alluxio
   */
  public LsCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "ls";
  }

  @Override
  protected int getNumOfArgs() {
    return 1;
  }

  @Override
  protected Options getOptions() {
    return new Options().addOption(RECURSIVE_OPTION).addOption(FORCE_OPTION);
  }

  /**
   * Displays information for all directories and files directly under the path specified in args.
   *
   * @param path The {@link AlluxioURI} path as the input of the command
   * @param recursive Whether list the path recursively
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException when non-Alluxio exception occurs
   */
  private void ls(AlluxioURI path, boolean recursive, boolean forceLoadMetadata)
      throws AlluxioException, IOException {
    ListStatusOptions options = ListStatusOptions.defaults();
    if (forceLoadMetadata) {
      options.setLoadMetadataType(LoadMetadataType.Always);
    }
    List<URIStatus> statuses = listStatusSortedByIncreasingCreationTime(path, options);
    for (URIStatus status : statuses) {
      System.out.format(formatLsString(SecurityUtils.isSecurityEnabled(), status.isFolder(),
          FormatUtils.formatMode((short) status.getMode(), status.isFolder()), status.getOwner(),
          status.getGroup(), status.getLength(), status.getCreationTimeMs(),
          100 == status.getInMemoryPercentage(), status.getPath()));
      if (recursive && status.isFolder()) {
        ls(new AlluxioURI(path.getScheme(), path.getAuthority(), status.getPath()), true,
            forceLoadMetadata);
      }
    }
  }

  private List<URIStatus> listStatusSortedByIncreasingCreationTime(AlluxioURI path,
      ListStatusOptions options)
      throws AlluxioException, IOException {
    List<URIStatus> statuses = mFileSystem.listStatus(path, options);
    Collections.sort(statuses, new Comparator<URIStatus>() {
      @Override
      public int compare(URIStatus status1, URIStatus status2) {
        long t1 = status1.getCreationTimeMs();
        long t2 = status2.getCreationTimeMs();
        if (t1 < t2) {
          return -1;
        }
        if (t1 == t2) {
          return 0;
        }
        return 1;
      }
    });
    return statuses;
  }

  @Override
  public void runCommand(AlluxioURI path, CommandLine cl) throws AlluxioException, IOException {
    ls(path, cl.hasOption("R"), cl.hasOption("f"));
  }

  @Override
  public String getUsage() {
    return "ls [-R] [-f] <path>";
  }

  @Override
  public String getDescription() {
    return "Displays information for all files and directories directly under the specified path."
        + " Specify -R to display files and directories recursively."
        + " Specify -f to force loading files in the directory.";
  }
}
