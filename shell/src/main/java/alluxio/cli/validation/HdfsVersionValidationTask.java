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
  public State validate(Map<String, String> optionMap) throws InterruptedException {
    return null;
  }

  private String getHadoopVersion() throws IOException {
    String[] cmd = new String[]{"hadoop", "version"};
    String version = ShellUtils.execCommand(cmd);
    System.out.println("Hadoop version " + version);
    return version;
  }

  public List<TaskResult> validateTasks() {
    List<TaskResult> results = new ArrayList<>();
    String taskName = "Validate hadoop version";
    // get hadoop version
    String hadoopVersion;
    try {
      hadoopVersion = getHadoopVersion();
    } catch (IOException e) {
      // log

      results.add(new TaskResult(State.FAILED, taskName,
              String.format("Failed to get hadoop version: %s.", e.getMessage()),
              "Please check if hadoop is on your PATH."));
      return results;
    }

    String version = mConf.get(PropertyKey.UNDERFS_VERSION);
    TaskResult result;
    if (version.equals(hadoopVersion)) {
      result = new TaskResult(State.OK, taskName,
              String.format("Hadoop version %s matches %s.",
                      hadoopVersion, PropertyKey.UNDERFS_VERSION.toString()),
              "");
    } else {
      result = new TaskResult(State.FAILED, taskName,
              String.format("Hadoop version %s does not match %s=%s.",
                      hadoopVersion, PropertyKey.UNDERFS_VERSION.toString(), version),
              String.format("Please configure %s to match the HDFS version.", PropertyKey.UNDERFS_VERSION.toString()));
    }
    results.add(result);
    return results;
  }
}
