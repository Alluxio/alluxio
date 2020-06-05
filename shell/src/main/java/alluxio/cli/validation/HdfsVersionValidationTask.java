package alluxio.cli.validation;

import alluxio.cli.ValidateUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ShellUtils;

import java.io.IOException;
import java.util.Map;

@ApplicableUfsType(ApplicableUfsType.Type.HDFS)
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
  public String getName() {
    return "ValidateHdfsVersion";
  }

  @Override
  public ValidateUtils.TaskResult validate(Map<String, String> optionMap) throws InterruptedException {
    // get hadoop version
    String hadoopVersion;
    try {
      hadoopVersion = getHadoopVersion();
    } catch (IOException e) {
      // log

      return new ValidateUtils.TaskResult(ValidateUtils.State.FAILED, getName(),
              String.format("Failed to get hadoop version: %s.", e.getMessage()),
              "Please check if hadoop is on your PATH.");
    }

    String version = mConf.get(PropertyKey.UNDERFS_VERSION);
    if (version.equals(hadoopVersion)) {
      return new ValidateUtils.TaskResult(ValidateUtils.State.OK, getName(),
              String.format("Hadoop version %s matches %s.",
                      hadoopVersion, PropertyKey.UNDERFS_VERSION.toString()),
              "");
    }

    return new ValidateUtils.TaskResult(ValidateUtils.State.FAILED, getName(),
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
