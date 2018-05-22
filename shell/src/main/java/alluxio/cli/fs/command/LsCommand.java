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
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.SecurityUtils;
import alluxio.wire.LoadMetadataType;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Displays information for the path specified in args. Depends on different options, this command
 * can also display the information for all directly children under the path, or recursively.
 */
@ThreadSafe
public final class LsCommand extends AbstractFileSystemCommand {
  public static final String IN_ALLUXIO_STATE_DIR = "DIR";
  public static final String IN_ALLUXIO_STATE_FILE_FORMAT = "%d%%";
  public static final String LS_FORMAT_PERMISSION = "%-11s";
  public static final String LS_FORMAT_FILE_SIZE = "%15s";
  public static final String LS_FORMAT_LAST_MODIFIED_TIME = "%24s";
  public static final String LS_FORMAT_ALLUXIO_STATE = "%5s";
  public static final String LS_FORMAT_PERSISTENCE_STATE = "%16s";
  public static final String LS_FORMAT_USER_NAME = "%-15s";
  public static final String LS_FORMAT_GROUP_NAME = "%-15s";
  public static final String LS_FORMAT_FILE_PATH = "%-5s";
  public static final String LS_FORMAT_NO_ACL = LS_FORMAT_FILE_SIZE + LS_FORMAT_PERSISTENCE_STATE
      + LS_FORMAT_LAST_MODIFIED_TIME + LS_FORMAT_ALLUXIO_STATE + " " + LS_FORMAT_FILE_PATH + "%n";
  public static final String LS_FORMAT = LS_FORMAT_PERMISSION + LS_FORMAT_USER_NAME
      + LS_FORMAT_GROUP_NAME + LS_FORMAT_FILE_SIZE + LS_FORMAT_PERSISTENCE_STATE
      + LS_FORMAT_LAST_MODIFIED_TIME + LS_FORMAT_ALLUXIO_STATE + " " + LS_FORMAT_FILE_PATH + "%n";

  private static final Option FORCE_OPTION =
      Option.builder("f")
          .required(false)
          .hasArg(false)
          .desc("force to load metadata for immediate children in a directory")
          .build();

  private static final Option LIST_DIR_AS_FILE_OPTION =
      Option.builder("d")
          .required(false)
          .hasArg(false)
          .desc("list directories as plain files")
          .build();

  private static final Option LIST_HUMAN_READABLE_OPTION =
      Option.builder("h")
          .required(false)
          .hasArg(false)
          .desc("print human-readable format sizes")
          .build();

  private static final Option LIST_PINNED_FILES_OPTION =
      Option.builder("p")
          .required(false)
          .hasArg(false)
          .desc("list all pinned files")
          .build();

  private static final Option RECURSIVE_OPTION =
      Option.builder("R")
          .required(false)
          .hasArg(false)
          .desc("list subdirectories recursively")
          .build();

  private static final Option SORT_OPTION =
      Option.builder()
          .required(false)
          .longOpt("sort")
          .hasArg(true)
          .desc("sort statuses by the given field "
                  + "{size|creationTime|inMemoryPercentage|lastModificationTime|name|path}")
          .build();

  private static final Option REVERSE_SORT_OPTION =
      Option.builder("r")
              .required(false)
              .hasArg(false)
              .desc("reverse order while sorting")
              .build();

  private static final Map<String, Comparator<URIStatus>> SORT_FIELD_COMPARATORS = new HashMap<>();

  static {
    SORT_FIELD_COMPARATORS.put("creationTime",
        Comparator.comparingLong(URIStatus::getCreationTimeMs));
    SORT_FIELD_COMPARATORS.put("inMemoryPercentage",
        Comparator.comparingLong(URIStatus::getInMemoryPercentage));
    SORT_FIELD_COMPARATORS.put("lastModificationTime",
        Comparator.comparingLong(URIStatus::getLastModificationTimeMs));
    SORT_FIELD_COMPARATORS.put("name",
        Comparator.comparing(URIStatus::getName, String.CASE_INSENSITIVE_ORDER));
    SORT_FIELD_COMPARATORS.put("path", Comparator.comparing(URIStatus::getPath));
    SORT_FIELD_COMPARATORS.put("size", Comparator.comparingLong(URIStatus::getBlockSizeBytes));
  }

  /**
   * Formats the ls result string.
   *
   * @param hSize print human-readable format sizes
   * @param acl whether security is enabled
   * @param isFolder whether this path is a file or a folder
   * @param permission permission string
   * @param userName user name
   * @param groupName group name
   * @param size size of the file in bytes
   * @param lastModifiedTime the epoch time in ms when the path is last modified
   * @param inAlluxioPercentage whether the file is in Alluxio
   * @param persistenceState the persistence state of the file
   * @param path path of the file or folder
   * @return the formatted string according to acl and isFolder
   */
  public static String formatLsString(boolean hSize, boolean acl, boolean isFolder, String
      permission,
      String userName, String groupName, long size, long lastModifiedTime, int inAlluxioPercentage,
      String persistenceState, String path) {
    String inAlluxioState;
    String sizeStr;
    if (isFolder) {
      inAlluxioState = IN_ALLUXIO_STATE_DIR;
      sizeStr = String.valueOf(size);
    } else {
      inAlluxioState = String.format(IN_ALLUXIO_STATE_FILE_FORMAT, inAlluxioPercentage);
      sizeStr = hSize ? FormatUtils.getSizeFromBytes(size) : String.valueOf(size);
    }

    if (acl) {
      return String.format(LS_FORMAT, permission, userName, groupName,
          sizeStr, persistenceState, CommonUtils.convertMsToDate(lastModifiedTime),
          inAlluxioState, path);
    } else {
      return String.format(LS_FORMAT_NO_ACL, sizeStr,
          persistenceState, CommonUtils.convertMsToDate(lastModifiedTime), inAlluxioState, path);
    }
  }

  private void printLsString(URIStatus status, boolean hSize) {
    System.out.print(formatLsString(hSize, SecurityUtils.isSecurityEnabled(),
        status.isFolder(), FormatUtils.formatMode((short) status.getMode(), status.isFolder()),
        status.getOwner(), status.getGroup(), status.getLength(),
        status.getLastModificationTimeMs(), status.getInAlluxioPercentage(),
        status.getPersistenceState(), status.getPath()));
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
  public Options getOptions() {
    return new Options()
        .addOption(FORCE_OPTION)
        .addOption(LIST_DIR_AS_FILE_OPTION)
        .addOption(LIST_HUMAN_READABLE_OPTION)
        .addOption(LIST_PINNED_FILES_OPTION)
        .addOption(RECURSIVE_OPTION)
        .addOption(REVERSE_SORT_OPTION)
        .addOption(SORT_OPTION);
  }

  /**
   * Displays information for all directories and files directly under the path specified in args.
   *
   * @param path The {@link AlluxioURI} path as the input of the command
   * @param recursive Whether list the path recursively
   * @param dirAsFile list the directory status as a plain file
   * @param hSize print human-readable format sizes
   * @param sortField sort the result by this field
   */
  private void ls(AlluxioURI path, boolean recursive, boolean forceLoadMetadata, boolean dirAsFile,
                  boolean hSize, boolean pinnedOnly, String sortField, boolean reverse)
      throws AlluxioException, IOException {
    if (dirAsFile) {
      URIStatus status = mFileSystem.getStatus(path);
      if (pinnedOnly && !status.isPinned()) {
        return;
      }
      printLsString(status, hSize);
      return;
    }

    ListStatusOptions options = ListStatusOptions.defaults();
    if (forceLoadMetadata) {
      options.setLoadMetadataType(LoadMetadataType.Always);
    }
    options.setRecursive(recursive);
    List<URIStatus> statuses = mFileSystem.listStatus(path, options);
    List<URIStatus> sorted = sortByFieldAndOrder(statuses, sortField, reverse);
    for (URIStatus status : sorted) {
      if (!pinnedOnly || status.isPinned()) {
        printLsString(status, hSize);
      }
    }
  }

  private List<URIStatus> sortByFieldAndOrder(
          List<URIStatus> statuses, String sortField, boolean reverse) throws IOException {
    Optional<Comparator<URIStatus>> sortToUse = Optional.ofNullable(
            SORT_FIELD_COMPARATORS.get(sortField));

    if (!sortToUse.isPresent()) {
      throw new InvalidArgumentException(ExceptionMessage.INVALID_ARGS_SORT_FIELD
          .getMessage(sortField));
    }

    Comparator<URIStatus> sortBy = sortToUse.get();
    if (reverse) {
      sortBy = sortBy.reversed();
    }

    return statuses.stream().sorted(sortBy).collect(Collectors.toList());
  }

  @Override
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException {
    ls(path, cl.hasOption("R"), cl.hasOption("f"), cl.hasOption("d"), cl.hasOption("h"),
        cl.hasOption("p"), cl.getOptionValue("sort", "path"), cl.hasOption("r"));
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
    return "ls [-d|-f|-p|-R|-h|--sort=option|-r] <path>";
  }

  @Override
  public String getDescription() {
    return "Displays information for all files and directories directly under the specified path, "
        + "including permission, owner, group, size (bytes for files or the number of children "
        + "for directories, persistence state, last modified time, the percentage of content"
        + " already in Alluxio and the path in order.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
  }
}
