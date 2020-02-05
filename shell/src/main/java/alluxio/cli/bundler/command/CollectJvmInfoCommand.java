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
import alluxio.util.ShellUtils;
import alluxio.util.SleepUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Command that collects information about the JVMs.
 * */
public class CollectJvmInfoCommand extends AbstractInfoCollectorCommand {
  public static final String COMMAND_NAME = "collectJvmInfo";
  private static final Logger LOG = LoggerFactory.getLogger(CollectJvmInfoCommand.class);
  private static final int COLLECT_JSTACK_TIMES = 3;
  private static final int COLLECT_JSTACK_INTERVAL = 3;

  private static final Option FORCE_OPTION =
          Option.builder("f")
                  .required(false)
                  .hasArg(false)
                  .desc("ignores existing work")
                  .build();

  /**
   * Creates an instance of {@link CollectJvmInfoCommand}.
   *
   * @param fsContext the {@link FileSystemContext} to execute in
   * */
  public CollectJvmInfoCommand(@Nullable FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public Options getOptions() {
    return new Options()
            .addOption(FORCE_OPTION);
  }

  @Override
  public String getCommandName() {
    return COMMAND_NAME;
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    int ret = 0;

    // Determine the working dir path
    String targetDir = getDestDir(cl);
    boolean force = cl.hasOption("f");

    // Skip if previous work can be reused.
    if (!force && foundPreviousWork(targetDir)) {
      LOG.info("Found previous work. Skipped.");
      return ret;
    }

    for (int i = 0; i < COLLECT_JSTACK_TIMES; i++) {
      LOG.info("Checking current JPS");
      Map<String, String> procs = getJps();
      String jstackContent = dumpJstack(procs);

      File outputFile = generateOutputFile(targetDir, String.format("%s-%s", getCommandName(), i));
      FileUtils.writeStringToFile(outputFile, jstackContent);

      // Interval
      LOG.info(String.format("Wait for an interval of %s seconds", COLLECT_JSTACK_INTERVAL));
      SleepUtils.sleepMs(LOG, COLLECT_JSTACK_INTERVAL * 1000);
    }

    // TODO(jiacheng); return code
    return ret;
  }

  /**
   * Gets a map of running JVMs by running the jps command.
   *
   * @return JVMs mapping to their process name
   * */
  public Map<String, String> getJps() throws IOException {
    Map<String, String> procs = new HashMap<>();

    // Get Jps output
    String[] jpsCommand = new String[]{"jps"};

    CommandReturn cr = ShellUtils.execCommandWithOutput(jpsCommand);
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

  /**
   * Dumps Jstack for each of the JVMs.
   *
   * @param procs JVMs mapping to their names
   * @return all outputs in String
   * */
  public String dumpJstack(Map<String, String> procs) throws IOException {
    // Output file
    StringWriter outputBuffer = new StringWriter();

    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    LocalDateTime now = LocalDateTime.now();
    String timeString = dtf.format(now);
    LOG.info(String.format("Dumping jstack time %s", timeString));
    outputBuffer.write(String.format("Dumping jstack at approximately %s", timeString));

    for (String k : procs.keySet()) {
      LOG.info("Dumping jstack on pid %s name %s", k, procs.get(k));
      outputBuffer.write(String.format("Jstack PID:%s Name:%s", k, procs.get(k)));

      String[] jstackCmd = new String[]{"jstack", k};
      CommandReturn cr = ShellUtils.execCommandWithOutput(jstackCmd);

      outputBuffer.write(cr.getFormattedOutput());
    }

    LOG.info("Jstack dump finished");
    return outputBuffer.toString();
  }

  @Override
  public String getUsage() {
    return null;
  }

  @Override
  public String getDescription() {
    return null;
  }
}
