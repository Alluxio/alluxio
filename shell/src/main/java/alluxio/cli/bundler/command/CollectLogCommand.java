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

import alluxio.client.file.FileSystemContext;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.util.CommonUtils;

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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

/**
 * Command to collect Alluxio logs.
 * */
public class CollectLogCommand  extends AbstractCollectInfoCommand {
  public static final String COMMAND_NAME = "collectLog";
  private static final Logger LOG = LoggerFactory.getLogger(CollectLogCommand.class);
  public static final Set<String> FILE_NAMES_PREFIXES = Stream.of(
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
  // We tolerate the beginning of a log file to contain some rows that are not timestamped.
  // A YARN application log can have >20 rows in the beginning for
  // general information about a job.
  // The timestamped log entries start after this general information block.
  private static final int TRY_PARSE_LOG_ROWS = 100;

  // Preserves the order of iteration, we try the longer pattern before the shorter one.
  // The 1st field is the DateTimeFormatter of a specific pattern.
  // The 2nd field is the length to take from the beginning of the log entry.
  // The length of the format string can be different from the datetime string it parses to.
  // For example, "yyyy-MM-dd'T'HH:mm:ss.SSSXX" has length of 27 but parses to
  // "2020-10-12T12:11:10.055+0800".
  // Note that the single quotes around 'T' are not in the real string,
  // and "XX" parses to the timezone, which is "+0800".
  // The datetime parsing works only when the string matches exactly to the format.
  private static final Map<DateTimeFormatter, Integer> FORMATTERS =
          new LinkedHashMap<DateTimeFormatter, Integer>(){
    {
      // "2020-01-03 12:10:11,874"
      put(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS"), 23);
      // "2020-01-03 12:10:11"
      put(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"), 19);
      // "2020-01-03 12:10"
      put(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"), 16);
      // "20/01/03 12:10:11"
      put(DateTimeFormatter.ofPattern("yy/MM/dd HH:mm:ss"), 17);
      // "20/01/03 12:10"
      put(DateTimeFormatter.ofPattern("yy/MM/dd HH:mm"), 14);
      // 2020-01-03T12:10:11.874+0800
      put(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXX"), 28);
      // 2020-01-03T12:10:11
      put(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"), 19);
      // 2020-01-03T12:10
      put(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm"), 16);
    }
  };

  private String mLogDirPath;
  private File mLogDir;
  private URI mLogDirUri;
  private Set<String> mIncludedPrefix;
  private Set<String> mExcludedPrefix;
  private LocalDateTime mStartTime;
  private LocalDateTime mEndTime;

  public static final String INCLUDE_OPTION_NAME = "include-logs";
  public static final String EXCLUDE_OPTION_NAME = "exclude-logs";
  public static final String ADDITIONAL_OPTION_NAME = "additional-logs";
  private static final Option INCLUDE_OPTION =
          Option.builder().required(false).argName("filename-prefixes")
                  .longOpt(INCLUDE_OPTION_NAME).hasArg(true)
                  .desc(String.format("extra log file name prefixes to include in "
                          + "${ALLUXIO_HOME}/logs. "
                          + "Only the files that start with the prefix will be included.%n"
                          + "This option should not be given in combination with --%s or --%s.%n"
                          + "<filename-prefixes> filename prefixes, separated by comma",
                          EXCLUDE_OPTION_NAME, ADDITIONAL_OPTION_NAME)).build();
  private static final Option EXCLUDE_OPTION =
          Option.builder().required(false).argName("filename-prefixes")
                  .longOpt(EXCLUDE_OPTION_NAME).hasArg(true)
                  .desc(String.format("log file name prefixes to exclude in ${ALLUXIO_HOME}/logs. "
                          + "The files that start with the prefix will be excluded.%n"
                          + "This will be checked before the additions defined in --%s.%n"
                          + "<filename-prefixes> filename prefixes, separated by comma",
                          ADDITIONAL_OPTION_NAME)).build();
  private static final Option ADDITIONAL_OPTION =
          Option.builder().required(false).argName("filename-prefixes")
                  .longOpt(ADDITIONAL_OPTION_NAME).hasArg(true)
                  .desc(String.format("extra log file name prefixes to include in "
                          + "${ALLUXIO_HOME}/logs. "
                          + "The files that start with the prefix will be included, "
                          + "in addition to the rest of regular logs like master.log.%n"
                          + "This will be checked after the exclusions defined in --%s.%n"
                          + "<filename-prefixes> filename prefixes, separated by comma",
                          EXCLUDE_OPTION_NAME)).build();
  private static final String START_OPTION_NAME = "start-time";
  private static final Option START_OPTION =
          Option.builder().required(false).argName("datetime")
                  .longOpt(START_OPTION_NAME).hasArg(true)
                  .desc("logs that do not contain entries after this time will be ignored\n"
                          + "<datetime> a datetime string like 2020-06-27T11:58:53 or "
                          + "\"2020-06-27 11:58:53\"").build();
  private static final String END_OPTION_NAME = "end-time";
  private static final Option END_OPTION =
          Option.builder().required(false).argName("datetime")
                  .longOpt(END_OPTION_NAME).hasArg(true)
                  .desc("logs that do not contain entries before this time will be ignored\n"
                          + "<datetime> a datetime string like 2020-06-27T11:58:53").build();
  // Class specific options are aggregated into CollectInfo with reflection
  public static final Options OPTIONS = new Options().addOption(INCLUDE_OPTION)
          .addOption(EXCLUDE_OPTION).addOption(ADDITIONAL_OPTION)
          .addOption(START_OPTION).addOption(END_OPTION);

  /**
   * Creates a new instance of {@link CollectLogCommand}.
   *
   * @param fsContext the {@link FileSystemContext} to execute in
   * */
  public CollectLogCommand(FileSystemContext fsContext) {
    super(fsContext);
    mLogDirPath = fsContext.getClusterConf().get(PropertyKey.LOGS_DIR);
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

    // TODO(jiacheng): phase 2 Copy intelligently find security risks
    mIncludedPrefix = new HashSet<>(FILE_NAMES_PREFIXES);
    boolean listReplaced = false;
    // Update the list according to the options
    if (cl.hasOption(INCLUDE_OPTION_NAME)) {
      Set<String> toInclude = parseFileNames(cl.getOptionValue(INCLUDE_OPTION_NAME));
      System.out.format("Only include the following filename prefixes: %s%n", toInclude);
      mIncludedPrefix = toInclude;
      listReplaced = true;
    }
    if (cl.hasOption(EXCLUDE_OPTION_NAME)) {
      if (listReplaced) {
        System.err.format("ERROR: Please do not use --%s when --%s is specified.%n",
                EXCLUDE_OPTION_NAME, INCLUDE_OPTION_NAME);
        return -1;
      }
      mExcludedPrefix = parseFileNames(cl.getOptionValue(EXCLUDE_OPTION_NAME));
      System.out.format("Exclude the following filename prefixes: %s%n", mExcludedPrefix);
    }
    if (cl.hasOption(ADDITIONAL_OPTION_NAME)) {
      if (listReplaced) {
        System.err.format("ERROR: Please do not use --%s when --%s is specified.%n",
                ADDITIONAL_OPTION_NAME, INCLUDE_OPTION_NAME);
        return -1;
      }
      Set<String> toInclude = parseFileNames(cl.getOptionValue(ADDITIONAL_OPTION_NAME));
      System.out.format("Additionally, include the following filename prefixes: %s%n", toInclude);
      mIncludedPrefix.addAll(toInclude);
    }

    // Check file timestamps
    boolean checkTimeStamp = false;
    if (cl.hasOption(START_OPTION_NAME)) {
      String startTimeStr = cl.getOptionValue(START_OPTION_NAME);
      mStartTime = parseDateTime(startTimeStr);
      System.out.format("Time window start: %s%n", mStartTime);
      checkTimeStamp = true;
    }
    if (cl.hasOption(END_OPTION_NAME)) {
      String endTimeStr = cl.getOptionValue(END_OPTION_NAME);
      mEndTime = parseDateTime(endTimeStr);
      System.out.format("Time window end: %s%n", mEndTime);
      checkTimeStamp = true;
    }
    if (mStartTime != null && mEndTime != null && mStartTime.isAfter(mEndTime)) {
      System.err.format("ERROR: Start time %s is later than end time %s!%n",
              mStartTime, mEndTime);
    }

    if (!mLogDir.exists()) {
      System.err.format("ERROR: Alluxio log directory %s does not exist!%n", mLogDirPath);
      return -1;
    }

    List<File> allFiles = CommonUtils.recursiveListLocalDir(mLogDir);
    for (File f : allFiles) {
      String relativePath = getRelativePathToLogDir(f);
      try {
        if (!shouldCopy(f, relativePath, checkTimeStamp)) {
          continue;
        }
        File targetFile = new File(mWorkingDirPath, relativePath);
        FileUtils.copyFile(f, targetFile, true);
      } catch (IOException e) {
        System.err.format("ERROR: file %s not found %s%n", f.getCanonicalPath(), e.getMessage());
      }
    }

    return 0;
  }

  private String getRelativePathToLogDir(File f) {
    return mLogDirUri.relativize(f.toURI()).getPath();
  }

  private boolean shouldCopy(File f, String relativePath, boolean checkTimeStamp)
          throws IOException {
    if (!fileNameIsWanted(relativePath)) {
      return false;
    }
    if (checkTimeStamp) {
      if (!fileTimeStampIsWanted(f)) {
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

  /**
   * Check if the file is wanted, based on the time window specified and the time of this file.
   * We assume we want the file unless we know the file is not wanted, based on the
   * specified time window.
   * We infer the end time from its last modified time, assuming that is the last log entry.
   * We infer the start time by parsing the first number of log entries, trying to identify
   * the timestamp on the log entry.
   * If we are not able to infer the start time, we assume it is LocalDateTime.MIN.
   * */
  private boolean fileTimeStampIsWanted(File f) throws IOException {
    if (mStartTime != null) {
      long timestamp = f.lastModified();
      LocalDateTime fileEndTime =
              LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
      // The file is earlier than the desired interval
      if (mStartTime.isAfter(fileEndTime)) {
        return false;
      }
    }
    if (mEndTime != null) {
      // Infer file start time by parsing the first bunch of rows
      LocalDateTime fileStartTime = inferFileStartTime(f);
      if (fileStartTime == null) {
        fileStartTime = LocalDateTime.MIN;
      }
      // The file is later than the desired interval
      if (mEndTime.isBefore(fileStartTime)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Infer the starting time of a log file by parsing the log entries from the beginning.
   * It will try the first certain lines with various known datetime patterns.
   *
   * @param f log file
   * @return the parsed datetime
   * */
  public static LocalDateTime inferFileStartTime(File f) throws FileNotFoundException {
    int r = 0;
    try (Scanner scanner = new Scanner(f)) {
      while (scanner.hasNextLine() && r < TRY_PARSE_LOG_ROWS) {
        String line = scanner.nextLine();
        LocalDateTime datetime = parseDateTime(line);
        if (datetime != null) {
          return datetime;
        }
        r++;
      }
    }
    return null;
  }

  private static Set<String> parseFileNames(String input) {
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

  /**
   * Identifies the datetime from a certain piece of log by trying various known patterns.
   * Returns null if unable to identify a datetime.
   *
   * @param s a log entry
   * @return identified datetime
   * */
  @Nullable
  public static LocalDateTime parseDateTime(String s) {
    for (Map.Entry<DateTimeFormatter, Integer> entry : FORMATTERS.entrySet()) {
      DateTimeFormatter fmt = entry.getKey();
      int len = entry.getValue();
      try {
        if (s.length() < len) {
          continue;
        }
        String datePart = s.substring(0, len);
        LocalDateTime datetime = LocalDateTime.parse(datePart, fmt);
        return datetime;
      } catch (DateTimeParseException e) {
        // It just means the string is not in this format
        continue;
      }
    }
    // Unknown format here
    LOG.warn("Unknown date format in {}", s.length() > 50 ? s.substring(0, 50) : s);
    return null;
  }
}
