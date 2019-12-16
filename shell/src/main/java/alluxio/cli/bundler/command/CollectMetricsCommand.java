package alluxio.cli.bundler.command;

import alluxio.client.file.FileSystemContext;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.UnavailableException;
import alluxio.util.SleepUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class CollectMetricsCommand extends AbstractInfoCollectorCommand {
  private static final Logger LOG = LoggerFactory.getLogger(CollectMetricsCommand.class);

  private static int COLLECT_METRIC_INTERVAL = 3;
  private static int COLLECT_METRIC_TIMES = 3;

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

  public CollectMetricsCommand(@Nullable FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "collectMetrics";
  }

  @Override
  public boolean hasSubCommand() {
    return false;
  }

  public String getMetricsJson() {
    // Generate URL from parameters
    String masterAddr;
    try {
      masterAddr = mFsContext.getMasterAddress().getHostName();
    } catch (UnavailableException e) {
      LOG.error("No Alluxio master available. Skip metrics collection.");
      e.printStackTrace();
      return String.format("%s", e.getStackTrace());
    }
    // TODO(jiacheng): Where to get /metrics/json/ ?
    String url = String.format("%s:%s/metrics/json/", masterAddr,
            mFsContext.getClusterConf().get(PropertyKey.MASTER_WEB_PORT));
    LOG.info(String.format("Metric address URL: %s", url));

    // Create an instance of HttpClient and do Http Get
    HttpClient client = new HttpClient();
    GetMethod method = new GetMethod(url);

    // Provide custom retry handler is necessary
    method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
            new DefaultHttpMethodRetryHandler(3, false));

    try {
      // Execute the method.
      int statusCode = client.executeMethod(method);

      return String.format("StatusCode: %s\nResponse%s", statusCode, new String(method.getResponseBody()));
    } catch (HttpException e) {
      LOG.error("Fatal protocol violation: " + e.getMessage());
      e.printStackTrace();
      return String.format("%s", e.getStackTrace());
    } catch (IOException e) {
      LOG.error("Fatal transport error: " + e.getMessage());
      e.printStackTrace();
      return String.format("%s", e.getStackTrace());
    } finally {
      // Release the connection.
      method.releaseConnection();
    }
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

    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    for (int i=0; i<COLLECT_METRIC_TIMES; i++) {
      LocalDateTime now = LocalDateTime.now();
      String timeString = dtf.format(now);
      LOG.info(String.format("Collecting metrics for %s", timeString));

      // Write to file
      File outputFile = getOutputFile(targetDir, String.format("%s-%s", getCommandName(), i));
      StringWriter outputBuffer = new StringWriter();
      outputBuffer.write(String.format("Collect metric at approximately %s", timeString));
      outputBuffer.write(getMetricsJson());
      FileUtils.writeStringToFile(outputFile, getMetricsJson());

      // Wait for an interval
      SleepUtils.sleepMs(LOG, 1000 * COLLECT_METRIC_INTERVAL);
    }

    return ret;
  }

  @Override
  public String getUsage() {
    return "collectMetrics";
  }

  @Override
  public String getDescription() {
    return "Collect Alluxio metrics";
  }
}
