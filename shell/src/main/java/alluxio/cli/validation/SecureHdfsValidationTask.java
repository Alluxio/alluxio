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

package alluxio.cli.validation;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.shell.CommandReturn;
import alluxio.util.ShellUtils;

import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Task for validating security configurations.
 */
public final class SecureHdfsValidationTask extends HdfsConfValidationTask {
  /**
   * Regular expression to parse principal used by Alluxio to connect to secure
   * HDFS.
   *
   * @see <a href="https://web.mit.edu/kerberos/krb5-1.5/krb5-1.5.4/doc/krb5-user/What-is-a-Kerberos-Principal_003f.html">Kerberos documentation</a>
   * for more details.
   */
  private static final Pattern PRINCIPAL_PATTERN =
      Pattern.compile("(?<primary>[\\w][\\w-]*\\$?)(/(?<instance>[\\w]+))?(@(?<realm>[\\w]+))?");

  private static final String PRINCIPAL_MAP_MASTER_KEY = "master";
  private static final String PRINCIPAL_MAP_WORKER_KEY = "worker";

  private static final Map<String, PropertyKey> PRINCIPAL_MAP = ImmutableMap.of(
      PRINCIPAL_MAP_MASTER_KEY, PropertyKey.MASTER_PRINCIPAL,
      PRINCIPAL_MAP_WORKER_KEY, PropertyKey.WORKER_PRINCIPAL);
  private static final Map<String, PropertyKey> KEYTAB_MAP = ImmutableMap.of(
      PRINCIPAL_MAP_MASTER_KEY, PropertyKey.MASTER_KEYTAB_KEY_FILE,
      PRINCIPAL_MAP_WORKER_KEY, PropertyKey.WORKER_KEYTAB_FILE);

  private final String mProcess;
  private PropertyKey mPrincipalProperty;
  private PropertyKey mKeytabProperty;
  private final AlluxioConfiguration mConf;

  /**
   * Constructor of {@link SecureHdfsValidationTask}
   * for validating Kerberos login ability.
   *
   * @param process type of the process on behalf of which this validation task is run
   * @param conf configuration
   */
  public SecureHdfsValidationTask(String process, AlluxioConfiguration conf) {
    super(conf);
    mConf = conf;
    mProcess = process.toLowerCase();
    mPrincipalProperty = PRINCIPAL_MAP.get(mProcess);
    mKeytabProperty = KEYTAB_MAP.get(mProcess);
  }

  @Override
  public State validate(Map<String, String> optionsMap) {
    if (shouldSkip()) {
      return State.SKIPPED;
    }
    if (super.validate(optionsMap) == State.FAILED) {
      return State.FAILED;
    }
    if (!validatePrincipalLogin()) {
      return State.FAILED;
    }
    return State.OK;
  }

  @Override
  protected boolean shouldSkip() {
    if (super.shouldSkip()) {
      return true;
    }
    String principal = null;
    if (mConf.isSet(mPrincipalProperty)) {
      principal = mConf.get(mPrincipalProperty);
    }
    if (principal == null || principal.isEmpty()) {
      System.out.format("Skip validation for secure HDFS. %s is not specified.%n",
          PRINCIPAL_MAP.get(mProcess).getName());
      return true;
    }
    return false;
  }

  private boolean validatePrincipalLogin() {
    // Check whether can login with specified principal and keytab
    String principal = mConf.get(mPrincipalProperty);
    Matcher matchPrincipal = PRINCIPAL_PATTERN.matcher(principal);
    if (!matchPrincipal.matches()) {
      System.err.format("Principal %s is not in the right format.%n", principal);
      return false;
    }
    String primary = matchPrincipal.group("primary");
    String instance = matchPrincipal.group("instance");
    String realm = matchPrincipal.group("realm");

    // Login with principal and keytab
    String keytab = mConf.get(mKeytabProperty);
    CommandReturn cr;
    String[] command = new String[] {"kinit", "-kt", keytab, principal};
    try {
      cr = ShellUtils.execCommandWithOutput(command);
      if (cr.getExitCode() != 0) {
        System.err.format("Kerberos login failed for %s with keytab %s with exit value %d.%n",
                principal, keytab, cr.getExitCode());
        System.err.format("Primary is %s, instance is %s and realm is %s.%n",
                primary, instance, realm);
        return false;
      }
    } catch (IOException e) {
      System.err.format("Failed to execute %s with exception: %s%n", Arrays.toString(command), e);
    }
    return true;
  }
}
