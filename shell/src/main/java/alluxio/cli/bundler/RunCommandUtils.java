package alluxio.cli.bundler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.Arrays;

public class RunCommandUtils {
  private static final Logger LOG = LoggerFactory.getLogger(RunCommandUtils.class);

  public static class CommandReturn {
    int mStatusCode;
    String mStdOut;
    String mStdErr;

    public CommandReturn(int code, String stdOut, String stdErr) {
      mStatusCode = code;
      mStdOut = stdOut;
      mStdErr = stdErr;
    }

    public String getStdOut() {
      return mStdOut;
    }

    public String getStdErr() {
      return mStdErr;
    }

    public int getStatusCode() {
      return mStatusCode;
    }

    public String getFormattedOutput() {
      return String.format("StatusCode:%s\nStdOut:\n%s\nStdErr:\n%s", this.getStatusCode(),
              this.getStdOut(), this.getStdErr());
    }
  }

  public static CommandReturn runCommandNoFail(String[] command) {
    LOG.info(String.format("Running command %s", Arrays.toString(command)));

    // Output buffer stream
    int ret = 0;
    StringWriter stdOut = new StringWriter();
    StringWriter stdErr = new StringWriter();
    try {
      Process p = Runtime.getRuntime().exec(command);

      BufferedReader stdInput = new BufferedReader(new
              InputStreamReader(p.getInputStream()));
      BufferedReader stdError = new BufferedReader(new
              InputStreamReader(p.getErrorStream()));

      // Read the output from the command
      String s;
      while ((s = stdInput.readLine()) != null) {
        stdOut.write(s);
      }

      // Read any errors from the attempted command
      while ((s = stdError.readLine()) != null) {
        stdErr.write(s);
      }

      ret = p.waitFor();
    }
    // TODO(jiacheng): Any exception to this?
    catch (Exception e) {
      stdErr.write(String.format("Failed to run %s, error: %s", Arrays.toString(command), e.getStackTrace()));
      // If return is not updated, change to 1.
      if (ret == 0) {
        ret = 1;
      }
    }

    LOG.info(String.format("Command finished with status %s", ret));
    return new CommandReturn(ret, stdOut.toString(), stdErr.toString());
  }
}
