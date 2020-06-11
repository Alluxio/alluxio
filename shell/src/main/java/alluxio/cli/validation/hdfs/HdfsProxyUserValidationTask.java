package alluxio.cli.validation.hdfs;

import alluxio.cli.ValidationUtils;
import alluxio.cli.validation.ApplicableUfsType;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.ImpersonationAuthenticator;
import alluxio.security.user.UserState;

import java.util.Map;

/**
 * Validates the proxyuser configurations for alluxio in hdfs.
 * */
@ApplicableUfsType(ApplicableUfsType.Type.HDFS)
public class HdfsProxyUserValidationTask extends HdfsConfValidationTask {
  /**
   * Creates a new instance of {@link HdfsProxyUserValidationTask}
   * for validating proxyuser configuration.
   *
   * @param path the UFS path
   * @param conf the UFS configuration
   */
  public HdfsProxyUserValidationTask(String path, AlluxioConfiguration conf) {
    super(path, conf);
  }

  @Override
  public String getName() {
    return "ValidateProxyUserConf";
  }

  private String getCurrentUser() throws UnauthenticatedException {
    UserState s = UserState.Factory.create(mConf);
    return s.getUser().getName();
  }

  private ValidationUtils.TaskResult validateProxyUsers(String userName) {
    String proxyUserKey = String.format("hadoop.proxyuser.%s.users", userName);
    String proxyGroupKey = String.format("hadoop.proxyuser.%s.groups", userName);
    String proxyUsers = mCoreConf.getOrDefault(proxyUserKey, "");
    String proxyGroups = mCoreConf.getOrDefault(proxyGroupKey, "");

    // Neither proxy users or groups is configured in core-site.xml
    if (proxyUsers.equals("") && proxyGroups.equals("")) {
      mMsg.append(String.format("Alluxio is running as user %s. But neither %s or %s is "
              + "configured in hadoop configuration. Alluxio is not able to perform "
              + "impersonation.%n", userName, proxyUserKey, proxyGroupKey));
      mAdvice.append(String.format("Please enable Alluxio user %s to impersonate.%n", userName));

      return new ValidationUtils.TaskResult(ValidationUtils.State.FAILED, getName(), mMsg.toString(),
              mAdvice.toString());
    }

    // If proxy users/groups is *, alluxio can impersonate anyone
    if (proxyUsers.equals(ImpersonationAuthenticator.WILDCARD)
            || proxyGroups.equals(ImpersonationAuthenticator.WILDCARD)) {
      mMsg.append(String.format("Alluxio user %s can impersonate as any user/group in HDFS.%n",
              userName));
      return new ValidationUtils.TaskResult(ValidationUtils.State.OK, getName(),
              mMsg.toString(), mAdvice.toString());
    }

    // There are proxyable users and groups for the Alluxio user in HDFS,
    // but we cannot know if that is a full set.
    // Leave a warning for the user to double check.
    // TODO(jiacheng): can we do better check than this?
    mMsg.append(String.format("Alluxio user %s has %s=%s and %s=%s set for HDFS.%n",
            userName, proxyUserKey, proxyUsers, proxyGroupKey, proxyGroups));
    mAdvice.append(String.format(
            "Please make sure that includes all users/groups Alluxio needs to impersonate as.%n"));
    return new ValidationUtils.TaskResult(ValidationUtils.State.WARNING, getName(),
            mMsg.toString(), mAdvice.toString());
  }

  @Override
  public ValidationUtils.TaskResult validate(Map<String, String> optionMap) {
    // Skip this test if NOSASL
    if (mConf.get(PropertyKey.SECURITY_AUTHENTICATION_TYPE)
            .equals(AuthType.NOSASL.getAuthName())) {
      return new ValidationUtils.TaskResult(ValidationUtils.State.SKIPPED, getName(),
              String.format("Impersonation validation is skipped for NOSASL"), "");
    }

    ValidationUtils.TaskResult loadConfig = loadHdfsConfig();
    if (loadConfig.getState() != ValidationUtils.State.OK) {
      return loadConfig;
    }

    // TODO(jiacheng): validate proxyuser.hosts for the cluster

    // Validate proxyuser config for the current Alluxio user
    try {
      String alluxioUser = getCurrentUser();
      return validateProxyUsers(alluxioUser);
    } catch (UnauthenticatedException e) {
      mMsg.append(String.format("Failed to authenticate in Alluxio: "));
      mMsg.append(ValidationUtils.getErrorInfo(e));
      mAdvice.append("Please fix the authentication issue.");
      return new ValidationUtils.TaskResult(ValidationUtils.State.FAILED, getName(),
              mMsg.toString(), mAdvice.toString());
    }
  }
}
