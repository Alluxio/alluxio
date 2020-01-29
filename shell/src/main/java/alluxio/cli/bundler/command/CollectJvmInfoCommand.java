package alluxio.cli.bundler.command;

import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
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

public class CollectJvmInfoCommand extends AbstractInfoCollectorCommand {
  private static final Logger LOG = LoggerFactory.getLogger(CollectJvmInfoCommand.class);

  private static int COLLECT_JSTACK_TIMES = 3;
  private static int COLLECT_JSTACK_INTERVAL = 3;

  private static final Option FORCE_OPTION =
          Option.builder("f")
                  .required(false)
                  .hasArg(false)
                  .desc("ignores existing work")
                  .build();

  @Override
  public Options getOptions() {
    return new Options()
            .addOption(FORCE_OPTION);
  }

  public CollectJvmInfoCommand(@Nullable FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "collectJvmInfo";
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

    for(int i = 0; i < COLLECT_JSTACK_TIMES; i++) {
      LOG.info("Checking current JPS");
      Map<String, String> procs = getJps();
      String jstackContent = dumpJstack(procs);

      File outputFile = getOutputFile(targetDir, String.format("%s-%s", getCommandName(), i));
      FileUtils.writeStringToFile(outputFile, jstackContent);

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
      RunCommandUtils.CommandReturn cr = RunCommandUtils.runCommandNoFail(jstackCmd);

      // Output
      String cmdResult = String.format("StatusCode:%s\nStdOut:\n%s\nStdErr:\n%s", cr.getStatusCode(),
              cr.getStdOut(), cr.getStdErr());
      outputBuffer.write(cmdResult);
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
