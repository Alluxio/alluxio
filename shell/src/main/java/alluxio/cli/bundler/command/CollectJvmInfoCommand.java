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
import alluxio.exception.AlluxioException;
import alluxio.shell.CommandReturn;
import alluxio.shell.ShellCommand;
import alluxio.util.ShellUtils;
import alluxio.util.SleepUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Command that collects information about the JVMs.
 * */
public class CollectJvmInfoCommand extends AbstractCollectInfoCommand {
  public static final String COMMAND_NAME = "collectJvmInfo";
  private static final Logger LOG = LoggerFactory.getLogger(CollectJvmInfoCommand.class);
  private static final int COLLECT_JSTACK_TIMES = 3;
  private static final int COLLECT_JSTACK_INTERVAL = 3 * 1000;

  /**
   * Creates an instance of {@link CollectJvmInfoCommand}.
   *
   * @param fsContext the {@link FileSystemContext} to execute in
   * */
  public CollectJvmInfoCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return COMMAND_NAME;
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    // Determine the working dir path
    mWorkingDirPath = getWorkingDirectory(cl);

    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    for (int i = 0; i < COLLECT_JSTACK_TIMES; i++) {
      // Use time as output file name
      LocalDateTime now = LocalDateTime.now();
      String timeString = dtf.format(now);
      LOG.info(String.format("Collecting JVM info at %s", timeString));

      LOG.info("Checking current JPS");
      Map<String, String> procs = getJps();
      String jstackContent = dumpJstack(procs);

      File outputFile = generateOutputFile(mWorkingDirPath,
              String.format("%s-%s", getCommandName(), timeString));
      FileUtils.writeStringToFile(outputFile, jstackContent);

      // Interval
      LOG.info(String.format("Wait for an interval of %s seconds", COLLECT_JSTACK_INTERVAL));
      SleepUtils.sleepMs(LOG, COLLECT_JSTACK_INTERVAL);
    }

    return 0;
  }

  private Map<String, String> getJps() throws IOException {
    Map<String, String> procs = new HashMap<>();

    // Attempt to sudo jps all existing JVMs
    ShellCommand sudoJpsCommand = new ShellCommand(new String[]{"sudo", "jps"});
    ShellCommand jpsCommand = new ShellCommand(new String[]{"jps"});

    CommandReturn cr = ShellUtils.execCmdWithBackup(sudoJpsCommand, jpsCommand);
    if (cr.getExitCode() != 0) {
      LOG.warn(cr.getFormattedOutput());
      return procs;
    }

    LOG.info("JPS succeeded");
    int idx = 0;
    for (String row : cr.getOutput().split("\n")) {
      String[] parts = row.split(" ");
      if (parts.length == 0) {
        LOG.error(String.format("Failed to parse row %s", row));
        continue;
      } else if (parts.length == 1) {
        // If the JVM has no name, assign it one.
        LOG.info(String.format("Row %s has no process name", row));
        procs.put(parts[0], "unknown" + idx);
        idx++;
      } else {
        LOG.info(String.format("Found JVM %s %s", parts[0], parts[1]));
        procs.put(parts[0], parts[1]);
      }
    }

    return procs;
  }

  private String dumpJstack(Map<String, String> procs) throws IOException {
    StringWriter outputBuffer = new StringWriter();

    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    LocalDateTime now = LocalDateTime.now();
    String timeString = dtf.format(now);
    String timeMsg = String.format("Dumping jstack at approximately %s", timeString);
    LOG.info(timeMsg);
    outputBuffer.write(timeMsg);

    for (Map.Entry<String, String> entry : procs.entrySet()) {
      String pid = entry.getKey();
      String pname = entry.getValue();
      String jstackMsg = String.format("Jstack PID:%s Name:%s", pid, pname);
      LOG.info(jstackMsg);
      outputBuffer.write(jstackMsg);

      // If normal jstack fails, attemp to sudo jstack this process
      ShellCommand jstackCmd = new ShellCommand(new String[]{"jstack", pid});
      ShellCommand sudoJstackCmd = new ShellCommand(new String[]{"sudo", "jstack", pid});
      CommandReturn cr = ShellUtils.execCmdWithBackup(jstackCmd, sudoJstackCmd);
      LOG.info("{} finished", Arrays.toString(cr.getCmd()));
      outputBuffer.write(cr.getFormattedOutput());
    }

    LOG.info("Jstack dump finished on all processes");
    return outputBuffer.toString();
  }

  @Override
  public String getUsage() {
    return "collectJvmInfo <outputPath>";
  }

  @Override
  public String getDescription() {
    return "Collect JVM information by collecting jstack";
  }
}
