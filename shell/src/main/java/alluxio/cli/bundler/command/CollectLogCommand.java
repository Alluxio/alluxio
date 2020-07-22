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

package alluxio.cli.bundler.command;

import alluxio.cli.bundler.CollectInfo;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;

import alluxio.util.CommonUtils;
import jline.internal.Nullable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Command to collect Alluxio logs.
 * */
public class CollectLogCommand  extends AbstractCollectInfoCommand {
  public static final String COMMAND_NAME = "collectLog";
  private static final Logger LOG = LoggerFactory.getLogger(CollectLogCommand.class);
  public static final Set<String> FILE_NAMES = Stream.of(
          "master.log",
          "master.out",
          "job_master.log",
          "job_master.out",
          "master_audit.log",
          "worker.log",
          "worker.out",
          "job_worker.log",
          "job_worker.out",
          "proxy.log",
          "proxy.out",
          "task.log",
          "task.out",
          "user"
  ).collect(Collectors.toSet());

  public static final String[] TIME_FORMATS = new String[]{
          "yyyy-MM-dd HH:mm:ss,SSS", // "2020-05-15 09:21:52,359"
          "yy/MM/dd HH:mm:ss", // "20/05/18 16:11:18"
          "yyyy-MM-dd'T'HH:mm:ss.SSSXX", // "2020-05-16T00:00:01.084+0800"
          "yyyy-MM-dd HH:mm:ss", // "2020-06-27 11:58:53"
          "yyyy-MM-dd HH:mm",
          "yyyy-MM-dd",
  };

  private String mLogDirPath;
  private File mLogDir;
  private URI mLogDirUri;
  private Set<String> mIncludedPrefix;
  private Set<String> mExcludedPrefix;
  private LocalDateTime mStartTime;
  private LocalDateTime mEndTime;

  public static final String INCLUDE_OPTION_NAME = "include-logs";
  private static final Option INCLUDE_OPTION =
          Option.builder().required(false).argName("i").longOpt(INCLUDE_OPTION_NAME).hasArg(true)
                  .desc("extra log file names to include in ${ALLUXIO_HOME}/logs").build();
  public static final String EXCLUDE_OPTION_NAME = "exclude-logs";
  private static final Option EXCLUDE_OPTION =
          Option.builder().required(false).argName("x").longOpt(EXCLUDE_OPTION_NAME).hasArg(true)
                  .desc("extra log file names to exclude in ${ALLUXIO_HOME}/logs").build();
  private static final String START_OPTION_NAME = "start-time";
  private static final Option START_OPTION =
          Option.builder().required(false).argName("s").longOpt(START_OPTION_NAME).hasArg(true)
                  .desc("").build();
  private static final String END_OPTION_NAME = "end-time";
  private static final Option END_OPTION =
          Option.builder().required(false).argName("e").longOpt(END_OPTION_NAME).hasArg(true)
                  .desc("").build();
  public static final Options OPTIONS = new Options().addOption(INCLUDE_OPTION).addOption(EXCLUDE_OPTION)
          .addOption(START_OPTION).addOption(END_OPTION);

  /**
   * Creates a new instance of {@link CollectLogCommand}.
   *
   * @param fsContext the {@link FileSystemContext} to execute in
   * */
  public CollectLogCommand(FileSystemContext fsContext) {
    super(fsContext);
    mLogDirPath = fsContext.getClusterConf().get(PropertyKey.LOGS_DIR);
    System.out.println("Log dir path: " + mLogDirPath);
    mLogDir = new File(mLogDirPath);
    mLogDirUri = mLogDir.toURI();
  }

  @Override
  public String getCommandName() {
    return COMMAND_NAME;
  }

  @Override
  public boolean hasSubCommand() {
    return false;
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    // Determine the working dir path
    mWorkingDirPath = getWorkingDirectory(cl);
    System.out.println("Target dir is " + mWorkingDirPath);

    System.out.format("Found options in CollectLogCommand: %s%n", Arrays.toString(cl.getOptions()));

    // TODO(jiacheng): phase 2 Copy intelligently find security risks
    // TODO(jiacheng): phase 2 components option
    mIncludedPrefix = new HashSet<>(FILE_NAMES);
    // Define whitelist and blacklist
    if (cl.hasOption(INCLUDE_OPTION_NAME)) {
      Set<String> toInclude = parseFileNames(cl.getOptionValue(INCLUDE_OPTION_NAME));
      System.out.println("Include the following filename prefixes: " + toInclude);
      mIncludedPrefix.addAll(toInclude);
    }
    if (cl.hasOption(EXCLUDE_OPTION_NAME)) {
      mExcludedPrefix = parseFileNames(cl.getOptionValue(EXCLUDE_OPTION_NAME));
      System.out.println("Exclude the following filename prefixes: " + mExcludedPrefix);
    }
    System.out.println("Target file names: " + mIncludedPrefix);

    // Check file timestamps
    boolean checkTimeStamp = false;
    if (cl.hasOption(START_OPTION_NAME)) {
      String startTimeStr = cl.getOptionValue(START_OPTION_NAME);
      mStartTime = parseDateTime(startTimeStr);
      checkTimeStamp = true;
    }
    if (cl.hasOption(END_OPTION_NAME)) {
      String endTimeStr = cl.getOptionValue(END_OPTION_NAME);
      mEndTime = parseDateTime(endTimeStr);
      checkTimeStamp = true;
    }

    if (!mLogDir.exists()) {
      System.err.format("ERROR: Alluxio log directory %s does not exist!%n", mLogDirPath);
      return -1;
    }

    List<File> allFiles = CommonUtils.recursiveListDir(mLogDir);
    for (File f : allFiles) {
      // Copy file
      String relativePath = getRelativePathToLogDir(f);
      System.out.println("Relative path against log dir: " + relativePath);
      if (!shouldCopy(f, relativePath, checkTimeStamp)) {
        continue;
      }
      File targetFile = new File(mWorkingDirPath, relativePath);
      System.out.format("Copy %s to %s%n", f.getCanonicalPath(), targetFile.getCanonicalPath());
      FileUtils.copyFile(f, targetFile, true);
    }

    return 0;
  }

  private String getRelativePathToLogDir(File f) {
    return mLogDirUri.relativize(f.toURI()).getPath();
  }

  private boolean shouldCopy(File f, String relativePath, boolean checkTimeStamp) {
    if (!fileNameIsWanted(relativePath)) {
      System.out.format("File %s is not wanted.%n", relativePath);
      return false;
    }
    if (checkTimeStamp) {
      System.out.println("Filter file by timestamp");
      if (!fileTimeStampIsWanted(f)) {
        System.out.println("File timestamp is out of range.");
        return false;
      }
    }
    return true;
  }

  private boolean fileNameIsWanted(String fileName) {
    if (mExcludedPrefix != null) {
      for (String x : mExcludedPrefix) {
        if (fileName.startsWith(x)) {
          return false;
        }
      }
    }
    for (String s : mIncludedPrefix) {
      if (fileName.startsWith(s)) {
        return true;
      }
    }
    return false;
  }

  private boolean fileTimeStampIsWanted(File f) {
    long timestamp = f.lastModified();
    LocalDateTime fileEndTime =
            LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
    System.out.println("File last modified time is " + fileEndTime);

    LocalDateTime fileStartTime = inferFileStartTime(f);
    if (fileStartTime == null) {
      fileStartTime = LocalDateTime.MIN;
    }
    System.out.format("File has start time %s and end time %s%n", fileStartTime, fileEndTime);

    // The file is earlier than the desired interval
    if (mStartTime != null && mStartTime.isAfter(fileEndTime)) {
      System.out.format("Wanted interval starts at %s, later than file last modified time %s%n", mStartTime, fileEndTime);
      return false;
    }
    // The file is later than the desired interval
    if (mEndTime != null && mEndTime.isBefore(fileStartTime)) {
      System.out.format("Wanted interval ends at %s, earlier than file first entry time %s%n", mEndTime, fileStartTime);
      return false;
    }
    return true;
  }

  public static LocalDateTime inferFileStartTime(File f) {
    int r = 0;
    try (Scanner scanner = new Scanner(f)){
      while (scanner.hasNextLine() && r < 30) {
        String line = scanner.nextLine();
        LocalDateTime datetime = parseDateTime(line);
        if (datetime != null) {
          System.out.format("Identified datetime %s on line %s%n", datetime, r);
          return datetime;
        }
        r++;
      }
    } catch (FileNotFoundException e) {
      // TODO(jiacheng)
      e.printStackTrace();
    }
    System.out.format("Datetime not found after %d rows.%n", r);
    return null;
  }

  private Set<String> parseFileNames(String input) {
    Set<String> names = new HashSet<>();
    names.addAll(Stream.of(input.split(",")).map(String::trim).collect(Collectors.toList()));
    return names;
  }

  @Override
  public String getUsage() {
    return "collectLogs <outputPath>";
  }

  @Override
  public String getDescription() {
    return "Collect Alluxio log files";
  }

  @Nullable
  public static LocalDateTime parseDateTime(String s) {
    for (String f : TIME_FORMATS) {
      DateTimeFormatter fmt = DateTimeFormatter.ofPattern(f);
      try {
        int len = f.length();
        if (f.endsWith("XX")) {
          len += 1; // "XX" in the format parses to timezone offset like "+0800"
          System.out.format("Format %s has len %s%n", f, len);
        }
        if (s.length() < len) {
          continue;
        }
        String datePart = s.substring(0, len);
        LocalDateTime datetime = LocalDateTime.parse(datePart, fmt);
        return datetime;
      } catch (DateTimeParseException e) {
        continue;
      }
    }
    // Unknown format here
    LOG.debug("Unknown date format in {}", s.length() > 50 ? s.substring(0, 50) : s);
    return null;
  }
}
