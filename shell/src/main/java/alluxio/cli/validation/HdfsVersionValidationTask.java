package alluxio.cli.validation;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.shell.CommandReturn;
import alluxio.util.ShellUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HdfsVersionValidationTask extends AbstractValidationTask {
  private final AlluxioConfiguration mConf;

  /**
   * Creates a new instance of {@link HdfsVersionValidationTask}
   * for validating HDFS version.
   * @param conf configuration
   */
  public HdfsVersionValidationTask(AlluxioConfiguration conf) {
    mConf = conf;
  }

  @Override
  public TaskResult validate(Map<String, String> optionMap) throws InterruptedException {
    // get hadoop version
    String hadoopVersion;
    try {
      hadoopVersion = getHadoopVersion();
    } catch (IOException e) {
      // log

      return new TaskResult(State.FAILED, mName,
              String.format("Failed to get hadoop version: %s.", e.getMessage()),
              "Please check if hadoop is on your PATH.");
    }

    String version = mConf.get(PropertyKey.UNDERFS_VERSION);
    if (version.equals(hadoopVersion)) {
      return new TaskResult(State.OK, mName,
              String.format("Hadoop version %s matches %s.",
                      hadoopVersion, PropertyKey.UNDERFS_VERSION.toString()),
              "");
    }

    return new TaskResult(State.FAILED, mName,
            String.format("Hadoop version %s does not match %s=%s.",
                    hadoopVersion, PropertyKey.UNDERFS_VERSION.toString(), version),
            String.format("Please configure %s to match the HDFS version.", PropertyKey.UNDERFS_VERSION.toString()));
  }

  protected String getHadoopVersion() throws IOException {
    String[] cmd = new String[]{"hadoop", "version"};
    String version = ShellUtils.execCommand(cmd);
    return version;
  }
}
