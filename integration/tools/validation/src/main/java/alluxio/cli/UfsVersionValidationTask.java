package alluxio.cli;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactoryRegistry;

import java.util.List;
import java.util.Map;

/**
 * This validates the UFS configuration for a users configured UFS path. It ensures that the
 * version of the UFS library is available (if configured).
 */
@ApplicableUfsType(ApplicableUfsType.Type.ALL)
public class UfsVersionValidationTask extends AbstractValidationTask {

  private final String mUfsPath;
  private final AlluxioConfiguration mConf;

    /**
     * Create a new instance of {@link UfsVersionValidationTask}.
     *
     * @param ufsPath the UFS URI to test
     * @param ufsConf the configuration for the UFS URI
     */
  public UfsVersionValidationTask(String ufsPath, AlluxioConfiguration ufsConf) {
    mUfsPath = ufsPath;
    mConf = ufsConf;
  }

  @Override
  protected ValidationTaskResult validateImpl(Map<String, String> optionMap) {
    UnderFileSystemConfiguration ufsConf =
          UnderFileSystemConfiguration.defaults(mConf).createMountSpecificConf(optionMap);
    String configuredVersion = mConf.get(PropertyKey.UNDERFS_VERSION);
    List<String> availableVersions =
        UnderFileSystemFactoryRegistry.getSupportedVersions(mUfsPath, ufsConf);
    ValidationTaskResult result = new ValidationTaskResult();
    result.setName(getName());
    result.setDesc("Validates that the configured UFS version exists as a library on the "
        + "system.");

    if (!mConf.isSetByUser(PropertyKey.UNDERFS_VERSION)) {
      result.setState(ValidationUtils.State.SKIPPED);
      result.setOutput("The UFS version was not configured by the user.");
    } else if (availableVersions.contains(configuredVersion)) {
      result.setState(ValidationUtils.State.OK);
      result.setOutput(String.format("The UFS path %s with configured version %s is "
          + "supported by the current installation", mUfsPath, configuredVersion));
    } else {
      result.setState(ValidationUtils.State.FAILED);

      if (availableVersions.size() > 0) {
        result.setOutput(String.format("UFS path %s was configured with version %s. The "
                + "supported versions on this system are: %s",
            mUfsPath, configuredVersion, availableVersions.toString()));
      } else {
        result.setOutput(String.format("UFS path %s was configured with version %s. This "
                + "path does not support UFS version configuration.",
            mUfsPath, configuredVersion));
      }
      result.setAdvice(String.format("Configured UFS version %s not available. Check that "
              + "the version is correct. Otherwise, consider using a different version.",
          configuredVersion));
    }
    return result;
  }

  @Override
  public String getName() {
    return "UfsVersionValidationTask";
  }
}
