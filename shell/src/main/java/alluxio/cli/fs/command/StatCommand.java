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
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Displays the path's info.
 * If path is a directory it displays the directory's info.
 * If path is a file, it displays the file's all blocks info.
 */
@ThreadSafe
@PublicApi
public final class StatCommand extends AbstractFileSystemCommand {
  /**
   * @param fsContext the filesystem of Alluxio
   */
  public StatCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "stat";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(
        Option.builder("f")
            .required(false)
            .hasArg()
            .desc("format")
            .build()
    );
  }

  @Override
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException {
    URIStatus status = mFileSystem.getStatus(path);
    if (cl.hasOption('f')) {
      System.out.println(formatOutput(cl, status));
    } else {
      if (status.isFolder()) {
        System.out.println(path + " is a directory path.");
        System.out.println(status);
      } else {
        System.out.println(path + " is a file path.");
        System.out.println(status);
        AlluxioBlockStore blockStore = AlluxioBlockStore.create(mFsContext);
        List<Long> blockIds = status.getBlockIds();
        if (blockIds.isEmpty()) {
          System.out.println("This file does not contain any blocks.");
        } else {
          System.out.println("Containing the following blocks: ");
          for (long blockId : blockIds) {
            System.out.println(blockStore.getInfo(blockId));
          }
        }
      }
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
    return "stat [-f <format>] <path>";
  }

  @Override
  public String getDescription() {
    return "Displays info for the specified path both file and directory."
        + " Specify -f to display info in given format:"
        + "   \"%N\": name of the file;"
        + "   \"%z\": size of file in bytes;"
        + "   \"%u\": owner;"
        + "   \"%g\": group name of owner;"
        + "   \"%y\" or \"%Y\": modification time,"
        + " %y shows 'yyyy-MM-dd HH:mm:ss' (the UTC date),"
        + " %Y it shows milliseconds since January 1, 1970 UTC;"
        + "   \"%b\": Number of blocks allocated for file";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }

  private static final String FORMAT_REGEX = "%([bguyzNY])";
  private static final Pattern FORMAT_PATTERN = Pattern.compile(FORMAT_REGEX);

  private String formatOutput(CommandLine cl, URIStatus status) {
    String format = cl.getOptionValue('f');
    int formatLen = format.length();

    StringBuilder output = new StringBuilder();
    Matcher m = FORMAT_PATTERN.matcher(format);
    int i = 0;
    while (i < formatLen && m.find(i)) {
      if (m.start() != i) {
        output.append(format.substring(i, m.start()));
      }
      output.append(getField(m, status));
      i = m.end();
    }
    if (i < formatLen) {
      output.append(format.substring(i));
    }
    return output.toString();
  }

  private String getField(Matcher m, URIStatus status) {
    char formatSpecifier = m.group(1).charAt(0);
    String resp = null;
    switch (formatSpecifier) {
      case 'b':
        resp = status.isFolder() ? "NA" : String.valueOf(status.getFileBlockInfos().size());
        break;
      case 'g':
        resp = status.getGroup();
        break;
      case 'u':
        resp = status.getOwner();
        break;
      case 'y':
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        resp = sdf.format(new Date(status.getLastModificationTimeMs()));
        break;
      case 'z':
        resp = status.isFolder() ? "NA" : String.valueOf(status.getLength());
        break;
      case 'N':
        resp = status.getName();
        break;
      case 'Y':
        resp = String.valueOf(status.getLastModificationTimeMs());
        break;
      default:
        Preconditions.checkArgument(false, "Unknown format specifier %c", formatSpecifier);
    }
    return resp;
  }
}
