/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.cli.hdfs;

import alluxio.cli.ValidationTaskResult;
import alluxio.cli.ValidationUtils;
import alluxio.cli.ApplicableUfsType;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ShellUtils;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Task for validating security configurations.
 */
@ApplicableUfsType(ApplicableUfsType.Type.HDFS)
public final class SecureHdfsValidationTask extends HdfsConfValidationTask {
  /**
   * Regular expression to parse principal used by Alluxio to connect to secure
   * HDFS.
   *
   * @see <a href="https://web.mit.edu/kerberos/krb5-1.5/krb5-1.5.4/doc/krb5-user/What-is-a-Kerberos-Principal_003f.html">Kerberos documentation</a>
   * for more details.
   */
  private static final Pattern PRINCIPAL_PATTERN =
      Pattern.compile("(?<primary>[^/@]*)(/(?<instance>[^/@]*))?@(?<realm>[^/@]*)");

  private static final String PRINCIPAL_MAP_MASTER_KEY = "master";
  private static final String PRINCIPAL_MAP_WORKER_KEY = "worker";

  private static final Map<String, PropertyKey> PRINCIPAL_MAP = ImmutableMap.of(
      PRINCIPAL_MAP_MASTER_KEY, PropertyKey.MASTER_PRINCIPAL,
      PRINCIPAL_MAP_WORKER_KEY, PropertyKey.WORKER_PRINCIPAL);
  private static final Map<String, PropertyKey> KEYTAB_MAP = ImmutableMap.of(
      PRINCIPAL_MAP_MASTER_KEY, PropertyKey.MASTER_KEYTAB_KEY_FILE,
      PRINCIPAL_MAP_WORKER_KEY, PropertyKey.WORKER_KEYTAB_FILE);

  // TODO(jiacheng): Use org.apache.hadoop.fs.CommonConfigurationKeysPublic for constants
  private static final String HDFS_AUTHENTICATION_KEY = "hadoop.security.authentication";
  private static final String HDFS_AUTHENTICATION_VALUE = "kerberos";
  private static final String DOC_LINK = "https://docs.alluxio.io/os/user/stable/en/ufs/HDFS.html"
          + "#connect-to-secure-hdfs";

  private final String mProcess;
  private PropertyKey mPrincipalProperty;
  private PropertyKey mKeytabProperty;
  private final AlluxioConfiguration mConf;
  private final StringBuilder mMsg;
  private final StringBuilder mAdvice;
  private final String mPath;

  /**
   * Constructor of {@link SecureHdfsValidationTask}
   * for validating Kerberos login ability.
   *
   * @param process type of the process on behalf of which this validation task is run
   * @param path the UFS path
   * @param conf configuration
   */
  public SecureHdfsValidationTask(String process, String path, AlluxioConfiguration conf) {
    super(path, conf);
    mConf = conf;
    mPath = path;
    mProcess = process.toLowerCase();
    mPrincipalProperty = PRINCIPAL_MAP.get(mProcess);
    mKeytabProperty = KEYTAB_MAP.get(mProcess);
    mMsg = new StringBuilder();
    mAdvice = new StringBuilder();
  }

  @Override
  public String getName() {
    return String.format("ValidateKerberosForSecureHdfs%s", StringUtils.capitalize(mProcess));
  }

  @Override
  public ValidationTaskResult validateImpl(Map<String, String> optionsMap) {
    if (!ValidationUtils.isHdfsScheme(mPath)) {
      mMsg.append("Skip this check as the UFS is not HDFS.\n");
      return new ValidationTaskResult(ValidationUtils.State.SKIPPED, getName(),
              mMsg.toString(), mAdvice.toString());
    }

    // superclass which uses its own msg and advice objects
    ValidationTaskResult loadConfig = loadHdfsConfig();
    if (loadConfig.getState() != ValidationUtils.State.OK) {
      String extraAdvice = "Validating a secure HDFS connection requires specifying additional "
          + "HDFS configuration files. ";
      return loadConfig.setAdvice(extraAdvice + loadConfig.getAdvice());
    }

    // The state is OK when the HDFS is secured
    ValidationTaskResult hdfsSecured = validateSecureHdfs();
    if (hdfsSecured.getState() != ValidationUtils.State.OK) {
      return hdfsSecured;
    }

    return validatePrincipalLogin();
  }

  private ValidationTaskResult validateSecureHdfs() {
    // Skipped if HDFS is not Kerberized
    // Ref: https://docs.cloudera.com/documentation/enterprise/5-16-x/topics
    // /cdh_sg_hadoop_security_enable.html
    String hadoopAuthentication = mCoreConf.getOrDefault(HDFS_AUTHENTICATION_KEY, "");
    boolean authenticationEnabled =
            hadoopAuthentication.equalsIgnoreCase(HDFS_AUTHENTICATION_VALUE);
    if (!authenticationEnabled) {
      mMsg.append("HDFS is not Kerberized. Skip this test.");
      return new ValidationTaskResult(ValidationUtils.State.SKIPPED, getName(),
              mMsg.toString(), mAdvice.toString());
    }
    return new ValidationTaskResult(ValidationUtils.State.OK, getName(),
            mMsg.toString(), mAdvice.toString());
  }

  private ValidationTaskResult validatePrincipalLogin() {
    String principal = mConf.getOrDefault(mPrincipalProperty, "");
    String keytab = mConf.getOrDefault(mKeytabProperty, "");

    if (principal.isEmpty() || keytab.isEmpty()) {
      mMsg.append(String.format("Failed to find Kerberos principal and keytab. "
              + "Found %s=%s and %s=%s.%n", mPrincipalProperty.toString(),
              principal, mKeytabProperty, keytab));
      mAdvice.append(String.format("Please configure Alluxio to connect with secure HDFS "
              + "following %s%n", DOC_LINK));
      return new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
              mMsg.toString(), mAdvice.toString());
    }

    // Check whether can login with specified principal and keytab
    Matcher matchPrincipal = PRINCIPAL_PATTERN.matcher(principal);
    if (!matchPrincipal.matches()) {
      mMsg.append(String.format("Principal %s is not in the right format.%n", principal));
      mAdvice.append(String.format("Please fix principal %s=%s.%n",
              mPrincipalProperty.toString(), principal));
      return new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
              mMsg.toString(), mAdvice.toString());
    }
    String primary = matchPrincipal.group("primary");
    String instance = matchPrincipal.group("instance");
    String realm = matchPrincipal.group("realm");

    // Login with principal and keytab
    String[] command = new String[] {"kinit", "-kt", keytab, principal};
    try {
      String output = ShellUtils.execCommand(command);
      mMsg.append(String.format("Command %s finished with output: %s%n",
              Arrays.toString(command), output));
      return new ValidationTaskResult(ValidationUtils.State.OK, getName(),
              mMsg.toString(), mAdvice.toString());
    } catch (IOException e) {
      mMsg.append(String.format("Kerberos login failed for %s with keytab %s.%n",
              principal, keytab));
      mMsg.append(ValidationUtils.getErrorInfo(e));
      mMsg.append(String.format("Primary is %s, instance is %s and realm is %s.%n",
              primary, instance, realm));
      ValidationTaskResult result = new ValidationTaskResult(
              ValidationUtils.State.FAILED, getName(), mMsg.toString(), mAdvice.toString());
      return result;
    }
  }
}
