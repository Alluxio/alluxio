package alluxio.cli.bundler.command;

import alluxio.cli.Command;
import alluxio.cli.bundler.InfoCollector;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.io.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.TimeUnit;

import alluxio.metrics.MetricsSystem;

import javax.annotation.Nullable;

public class CollectMetricsCommand extends AbstractInfoCollectorCommand {
  private static int METRICS_COLLECTION_INTERVAL = 3;

  public CollectMetricsCommand(@Nullable InstancedConfiguration conf) {
    super(conf);
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
    // Generate URL
    // TODO(jiacheng): master addr
    String url = String.format("localhost:%s/metrics/json/", mConf.get(PropertyKey.MASTER_WEB_PORT));

    // Create an instance of HttpClient.
    HttpClient client = new HttpClient();

    // Create a method instance.
    GetMethod method = new GetMethod(url);

    // Provide custom retry handler is necessary
    method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
            new DefaultHttpMethodRetryHandler(3, false));

    try {
      // Execute the method.
      int statusCode = client.executeMethod(method);

      if (statusCode != HttpStatus.SC_OK) {
        System.err.println("Method failed: " + method.getStatusLine());
      }

      // Read the response body.
      byte[] responseBody = method.getResponseBody();

      // Deal with the response.
      // Use caution: ensure correct character encoding and is not binary data
      System.out.println(new String(responseBody));

      return new String(responseBody);
    } catch (HttpException e) {
      System.err.println("Fatal protocol violation: " + e.getMessage());
      e.printStackTrace();
    } catch (IOException e) {
      System.err.println("Fatal transport error: " + e.getMessage());
      e.printStackTrace();
    } finally {
      // Release the connection.
      method.releaseConnection();
    }

    // TODO(jiacheng): how to handle this
    return "";
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    // Write to file
    File file1 = new File(getOutputPath());
    FileUtils.writeStringToFile(file1, getMetricsJson());

    try {
      TimeUnit.SECONDS.sleep(METRICS_COLLECTION_INTERVAL);
    } catch(InterruptedException ex) {
      // TODO(jiacheng): What do i do?
    }

    File file2 = new File(getOutputPath());
    FileUtils.writeStringToFile(file2, getMetricsJson());

    return 0;
  }

  private String getOutputPath() {
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    LocalDateTime now = LocalDateTime.now();
    return Paths.get(this.getWorkingDirectory(), this.getCommandName(), "metric_" + dtf.format(now)).toString();
  }

  @Override
  public String getUsage() {
    return "collectMetrics";
  }

  @Override
  public String getDescription() {
    return "Collect Alluxio metrics";
  }

  @Override
  public String getWorkingDirectory() {
    // TODO(jiacheng): create if not exist
    return Paths.get(super.getWorkingDirectory(), this.getCommandName()).toString();
  }
}
