package alluxio.cli.validation;

import alluxio.cli.ValidateUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.security.authentication.ImpersonationAuthenticator;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Validates the impersonation configurations in alluxio and hdfs.
 * */
@ApplicableUfsType(ApplicableUfsType.Type.HDFS)
public class HdfsImpersonationValidationTask extends HdfsConfValidationTask {
  private final Map<String, Set<String>> mImpersonationUsers;
  private final Map<String, Set<String>> mImpersonationGroups;
  private final Mode mMode;

  /**
   * Creates a new instance of {@link HdfsImpersonationValidationTask}
   * for validating impersonation configuration.
   *
   * @param path the UFS path
   * @param conf the UFS configuration
   * @param mode the mode for validation
   */
  public HdfsImpersonationValidationTask(String path, AlluxioConfiguration conf, Mode mode) {
    super(path, conf);
    mMode = mode;
    ImpersonationAuthenticator ia = new ImpersonationAuthenticator(mConf);
    mImpersonationUsers = ia.getImpersonationUsers();
    mImpersonationGroups = ia.getmImpersonationGroups();
  }

  @Override
  public String getName() {
    return String.format("ValidateImpersonationConf%s", mMode);
  }

  private boolean shouldSkip() {
    // If no impersonation setting in Alluxio, skip the check
    if (mImpersonationUsers.entrySet().size() == 0
            && mImpersonationGroups.size() == 0) {
      mMsg.append("No impersonation setting found in Alluxio. "
              + "Skip the impersonation validation step.\n");
      return true;
    }
    return false;
  }

  private ValidateUtils.TaskResult validateImpersonationUsers() {
    String taskName = "Validate alluxio impersonation users";
    ValidateUtils.State state = ValidateUtils.State.OK;
    StringBuilder msg = new StringBuilder();
    StringBuilder advice = new StringBuilder();
    for (Map.Entry<String, Set<String>> entry : mImpersonationUsers.entrySet()) {
      String userName = entry.getKey();
      Set<String> impUsers = entry.getValue();
      PropertyKey alluxioKey =
              PropertyKey.Template.MASTER_IMPERSONATION_USERS_OPTION.format(userName);
      String hdfsKey = String.format("hadoop.proxyuser.%s.users", userName);
      msg.append(String.format("User %s has impersonation configured in Alluxio property %s=%s. %n",
              userName, alluxioKey.toString(), mConf.get(alluxioKey)));

      // The impersonation user is not configured in core-site.xml
      if (!mCoreConf.containsKey(hdfsKey)) {
        state = ValidateUtils.State.FAILED;
        msg.append(String.format("But %s is not configured in hadoop proxyuser.%n",
                hdfsKey));
        advice.append(String.format("Please configure %s to match %s.%n",
                hdfsKey, alluxioKey.toString()));
        continue;
      }
      String hdfsImpUsers = mCoreConf.get(hdfsKey);

      // Consider wildcard separately
      if (impUsers.contains(ImpersonationAuthenticator.WILDCARD)) {
        if (hdfsImpUsers.equals(ImpersonationAuthenticator.WILDCARD)) {
          // If impersonation is enabled for all users in Alluxio and HDFS, succeed
          msg.append(String.format("User %s can impersonate any user in Alluxio and HDFS.%n",
                  userName));
        } else {
          state = ValidateUtils.State.FAILED;
          msg.append(String.format("User %s can impersonate any user in "
                  + "Alluxio but only %s in HDFS.%n", userName, hdfsImpUsers));
          advice.append(String.format("Please set %s to %s. ",
                  hdfsKey, ImpersonationAuthenticator.WILDCARD));
        }
        continue;
      } else if (hdfsImpUsers.equals(ImpersonationAuthenticator.WILDCARD)) {
        msg.append(String.format("User %s can impersonate any user in HDFS.%n", userName));
        continue;
      }

      // Not using wildcard, compare the exact usernames
      Set<String> nameSet = new HashSet<>(Arrays.asList(hdfsImpUsers.split(",")));
      System.out.format("Impersonable users: %s%n", nameSet);
      // The proxyuser can be enabled to impersonate more users than defined in Alluxio
      Set<String> missedUsers = Sets.difference(impUsers, nameSet); // in alluxio not in hdfs
      System.out.format("Found missed users %s%n", missedUsers);
      if (missedUsers.size() > 0) {
        state = ValidateUtils.State.FAILED;
        msg.append(String.format("User %s can impersonate as users %s in Alluxio but "
                + "not in HDFS.%n", userName, missedUsers));
        advice.append(String.format("Please add the missing users to %s. ", hdfsKey));
        continue;
      }

      // All checks passed
      msg.append("All impersonable users in Alluxio are found in HDFS. \n");
    }

    return new ValidateUtils.TaskResult(state, taskName, msg.toString(), advice.toString());
  }

  // TODO(jiacheng): refactor with users logic
  private ValidateUtils.TaskResult validateImpersonationGroups() {
    String taskName = "Validate alluxio impersonation groups";
    ValidateUtils.State state = ValidateUtils.State.OK;
    StringBuilder msg = new StringBuilder();
    StringBuilder advice = new StringBuilder();
    for (Map.Entry<String, Set<String>> entry : mImpersonationGroups.entrySet()) {
      String userName = entry.getKey();
      Set<String> impGroups = entry.getValue();
      PropertyKey alluxioKey = PropertyKey.Template
              .MASTER_IMPERSONATION_GROUPS_OPTION.format(userName);
      String hdfsKey = String.format("hadoop.proxyuser.%s.groups", userName);
      msg.append(String.format("User %s has impersonation configured in Alluxio property %s=%s. ",
              userName, alluxioKey.toString(), mConf.get(alluxioKey)));

      // The impersonation group is not configured in core-site.xml
      if (!mCoreConf.containsKey(hdfsKey)) {
        state = ValidateUtils.State.FAILED;
        msg.append(String.format("But %s is not configured in hadoop proxyuser. ", hdfsKey));
        advice.append(String.format("Please configure %s to match %s",
                hdfsKey, alluxioKey.toString()));
        continue;
      }
      String hdfsImpGroups = mCoreConf.get(hdfsKey);

      // Consider wildcard separately
      if (impGroups.contains(ImpersonationAuthenticator.WILDCARD)) {
        if (hdfsImpGroups.equals(ImpersonationAuthenticator.WILDCARD)) {
          // If impersonation is enabled for all users in Alluxio and HDFS, succeed
          msg.append(String.format("User %s can impersonate any group in "
                  + "Alluxio and HDFS. ", userName));
        } else {
          msg.append(String.format("User %s can impersonate any group "
                  + "in Alluxio but not in HDFS. ", userName));
          advice.append(String.format("Please set %s to %s. ",
                  hdfsKey, ImpersonationAuthenticator.WILDCARD));
        }
        continue;
      } else if (hdfsImpGroups.equals(ImpersonationAuthenticator.WILDCARD)) {
        msg.append(String.format("User %s can impersonate any group in HDFS.%n", userName));
        continue;
      }

      // The impersonation group has different configuration in core-site.xml
      Set<String> nameSet = new HashSet<>(Arrays.asList(hdfsImpGroups.split(",")));
      System.out.format("Impersonable groups: %s%n", nameSet);
      // The proxyuser can be enabled to impersonate more groups than defined in Alluxio
      Set<String> missedGroups = Sets.difference(impGroups, nameSet); // in alluxio not in hdfs
      System.out.format("Found missed groups %s%n", missedGroups);
      if (missedGroups.size() > 0) {
        state = ValidateUtils.State.FAILED;
        msg.append(String.format("User %s can impersonate as groups %s "
                + "in Alluxio but not in HDFS.", userName, missedGroups));
        advice.append(String.format("Please add the missing groups to %s. ", hdfsKey));
        continue;
      }

      // All checks passed
      msg.append(String.format("Found matching configuration in %s and %s. ",
              alluxioKey.toString(), hdfsKey));
    }
    return new ValidateUtils.TaskResult(state, taskName, msg.toString(), advice.toString());
  }

  // At least the current host should be in hadoop.proxy.<username>.hosts
  private ValidateUtils.TaskResult validateImpersonationHosts() {
    String taskName = "Validate proxyuser hosts";
    ValidateUtils.State state = ValidateUtils.State.OK;
    StringBuilder msg = new StringBuilder();
    StringBuilder advice = new StringBuilder();

    // current host
    String localhost = NetworkAddressUtils.getLocalHostName(1000);

    // All the possible users Alluxio needs
    Set<String> allUsers = Sets.union(mImpersonationUsers.keySet(), mImpersonationGroups.keySet());
    for (String userName : allUsers) {
      msg.append(String.format("User %s is configured to allow impersonation is Alluxio. ",
              userName));

      String hdfsKey = String.format("hadoop.proxyuser.%s.hosts", userName);
      if (!mCoreConf.containsKey(hdfsKey)) {
        state = ValidateUtils.State.FAILED;
        msg.append("But the user is not allowed to use impersonation on this host.");
        advice.append(String.format("Please configure %s to contain %s. ", hdfsKey, localhost));
        continue;
      }

      String proxyHosts = mCoreConf.get(hdfsKey);
      // Consider wildcard separately
      if (proxyHosts.equals(ImpersonationAuthenticator.WILDCARD)) {
        msg.append(String.format("The user is enabled for impersonation from all hosts. "));
        continue;
      }
      // If wildcard is not used and the localhost is not in the permitted list
      if (!proxyHosts.contains(localhost)) {
        state = ValidateUtils.State.FAILED;
        msg.append(String.format("But %s does not contain host %s. ", hdfsKey, localhost));
        advice.append(String.format("Please enable host %s in %s. ", localhost, hdfsKey));
        continue;
      }

      // Passed all checks
      msg.append(String.format("Host %s is enabled to use impersonation in HDFS. ", localhost));
    }
    return new ValidateUtils.TaskResult(state, taskName, msg.toString(), advice.toString());
  }

  @Override
  public ValidateUtils.TaskResult validate(Map<String, String> optionMap) {
    if (shouldSkip()) {
      return new ValidateUtils.TaskResult(ValidateUtils.State.SKIPPED, getName(),
              mMsg.toString(), mAdvice.toString());
    }

    ValidateUtils.TaskResult loadConfig = loadHdfsConfig();
    if (loadConfig.getState() != ValidateUtils.State.OK) {
      return loadConfig;
    }

    // TODO(jiacheng): do we want to check the current user even if
    //  there's no impersonation setting?
    switch (mMode) {
      case USERS:
        return validateImpersonationUsers();
      case GROUPS:
        return validateImpersonationGroups();
      case HOSTS:
        return validateImpersonationHosts();
      default:
        throw new RuntimeException(String.format("Unknown validation mode %s", mMode));
    }
  }

  /**
   * The mode for impersonation check.
   * */
  public enum Mode {
    USERS,
    GROUPS,
    HOSTS
  }
}
