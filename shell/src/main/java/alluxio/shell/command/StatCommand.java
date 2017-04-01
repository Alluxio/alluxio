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
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Displays the path's info.
 * If path is a directory it displays the directory's info.
 * If path is a file, it displays the file's all blocks info.
 */
@ThreadSafe
public final class StatCommand extends WithWildCardPathCommand {
  /**
   * @param fs the filesystem of Alluxio
   */
  public StatCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "stat";
  }

  @Override
  protected Options getOptions() {
    return new Options().addOption(
            Option.builder("f")
                    .required(false)
                    .hasArg()
                    .desc("format")
                    .build()
    );
  }

  @Override
  protected void runCommand(AlluxioURI path, CommandLine cl) throws AlluxioException, IOException {
    URIStatus status = mFileSystem.getStatus(path);
    if (status.isFolder()) {
      System.out.println(path + " is a directory path.");
      System.out.println(status);
    } else {
      System.out.println(path + " is a file path.");
      if (cl.hasOption('f')) {
        System.out.println(formatOutput(cl, status));
      } else {
        System.out.println(status);
      }
      System.out.println("Containing the following blocks: ");
      AlluxioBlockStore blockStore = AlluxioBlockStore.create();
      for (long blockId : status.getBlockIds()) {
        System.out.println(blockStore.getInfo(blockId));
      }
    }
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

  private static final String FORMAT_REGEX = "%([bgruyzNY])";
  private static final Pattern FORMAT_PATTERN = Pattern.compile(FORMAT_REGEX);

  private String formatOutput(CommandLine cl, URIStatus status) {
    String format = cl.getOptionValue('f');
    int formatLen = format.length();

    StringBuilder output = new StringBuilder();
    Matcher m = FORMAT_PATTERN.matcher(format);
    for (int i = 0; i < formatLen;) {
      if (m.find(i)) {
        if (m.start() != i) {
          output.append(format.substring(i, m.start()));
        }
        output.append(getField(m, status));
        i = m.end();
      } else {
        output.append(format.substring(i));
        break;
      }
    }
    return output.toString();
  }

  private String getField(Matcher m, URIStatus status) {
    char formatSpecifier = m.group(1).charAt(0);
    switch (formatSpecifier) {
      case 'b':
        return String.valueOf(status.getFileBlockInfos().size());
      case 'g':
        return status.getGroup();
      case 'u':
        return status.getOwner();
      case 'y':
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(new Date(status.getLastModificationTimeMs()));
      case 'z':
        return String.valueOf(status.getLength());
      case 'N':
        return status.getName();
      case 'Y':
        return String.valueOf(status.getLastModificationTimeMs());
      default:
        assert false;
        return "";
    }
  }
}
