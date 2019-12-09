package alluxio.cli.bundler.command;

import alluxio.cli.bundler.RunCommandUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.util.SleepUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CollectJvmInfoCommand extends AbstractInfoCollectorCommand {
  private static final Logger LOG = LoggerFactory.getLogger(CollectJvmInfoCommand.class);

  private static int COLLECT_JSTACK_TIMES = 3;
  private static int COLLECT_JSTACK_INTERVAL = 3;

  public CollectJvmInfoCommand(@Nullable InstancedConfiguration conf) {
    super(conf);
  }

  @Override
  public String getCommandName() {
    return "collectJvmInfo";
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
      int ret = 0;
      for(int i = 0; i < COLLECT_JSTACK_TIMES; i++) {
        LOG.info("Checking current JPS");
        Map<String, String> procs = getJps();


        // TODO(jiacheng): ret value
        dumpJstack(procs);

        // Interval
        LOG.info(String.format("Wait for an interval of %s seconds", COLLECT_JSTACK_INTERVAL));
        SleepUtils.sleepMs(LOG, COLLECT_JSTACK_INTERVAL * 1000);
      }

      // TODO(jiacheng); return code
      return ret;
  }

  public Map<String, String> getJps() {
    Map<String, String> procs = new HashMap<>();

    // Get Jps output
    String[] jpsCommand = new String[]{"jps"};

    RunCommandUtils.CommandReturn cr = RunCommandUtils.runCommandNoFail(jpsCommand);
    if (cr.getStatusCode() != 0) {
      LOG.error(String.format("JPS returned status %s and stderr:\n%s", cr.getStatusCode(), cr.getStdErr()));
      return procs;
    }

    LOG.info("JPS succeeded");
    // TODO(jiacheng): stderr?
    for (String row : cr.getStdOut().split("\n")) {
      String[] parts = row.split(" ");
      if (parts.length == 0) {
        LOG.error(String.format("Failed to parse row %s", row));
        continue;
      } else if (parts.length == 1) {
        LOG.info(String.format("Row %s has no process name", row));
        procs.put(parts[0], "unknown");
      } else {
        LOG.info(String.format("Found JVM %s %s", parts[0], parts[1]));
        procs.put(parts[0], parts[1]);
      }
    }

    return procs;
  }

  public void dumpJstack(Map<String, String> procs) throws IOException {
    // Output file
    StringWriter outputBuffer = new StringWriter();
    String outputFilePath = Paths.get(this.getWorkingDirectory(), getOutputPath()).toString();
    File outputFile = new File(outputFilePath);

    for (String k : procs.keySet()) {
      LOG.info("Dumping jstack on pid %s name %s", k, procs.get(k));
      outputBuffer.write(String.format("Jstack PID:%s Name:%s", k, procs.get(k)));

      String[] jstackCmd = new String[]{"jstack", k};
      RunCommandUtils.CommandReturn cr = RunCommandUtils.runCommandNoFail(jstackCmd);

      // Output
      String cmdResult = String.format("StatusCode:%s\nStdOut:\n%s\nStdErr:\n%s", cr.getStatusCode(),
              cr.getStdOut(), cr.getStdErr());
      try {
        FileUtils.writeStringToFile(outputFile, cmdResult);
      } catch (IOException e) {
        LOG.error(String.format("Failed to output jstack to %s", outputFilePath));
        e.printStackTrace();
      }
    }

    return;
  }

  private String getOutputPath() {
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    LocalDateTime now = LocalDateTime.now();
    return Paths.get(this.getWorkingDirectory(), this.getCommandName(), "jstacks_" + dtf.format(now)).toString();
  }

  @Override
  public String getWorkingDirectory() {
    // TODO(jiacheng): create if not exist
    return Paths.get(super.getWorkingDirectory(), this.getCommandName()).toString();
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
