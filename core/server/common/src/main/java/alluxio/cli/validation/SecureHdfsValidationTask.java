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

import alluxio.Configuration;
import alluxio.PropertyKey;

import org.apache.commons.cli.Options;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Task for validating security configurations.
 */
public class SecureHdfsValidationTask extends HdfsValidationTask {
  /**
   * Regular expression to parse principal used by Alluxio to connect to secure
   * HDFS.
   *
   * @see <a href="https://web.mit.edu/kerberos/krb5-1.5/krb5-1.5.4/doc/krb5-user/What-is-a-Kerberos-Principal_003f.html">Kerberos documentation</a>
   * for more details.
   */
  private static final Pattern PRINCIPAL_PATTERN =
      Pattern.compile("(?<primary>[\\w][\\w-]*\\$?)(/(?<instance>[\\w]+))?(@(?<realm>[\\w]+))?");

  private static final Map<String, PropertyKey> PRINCIPAL_MAP = ImmutableMap.of(
      "master", PropertyKey.MASTER_PRINCIPAL,
      "worker", PropertyKey.WORKER_PRINCIPAL);
  private static final Map<String, PropertyKey> KEYTAB_MAP = ImmutableMap.of(
      "master", PropertyKey.MASTER_KEYTAB_KEY_FILE,
      "worker", PropertyKey.WORKER_KEYTAB_FILE);

  /**
   * Constructor of {@link SecureHdfsValidationTask}.
   *
   * @param process type of the process on behalf of which this validation task is run
   */
  public SecureHdfsValidationTask(String process) {
    super(process);
  }

  @Override
  public boolean validate(Map<String, String> optionsMap) {
    if (!super.validate(optionsMap)) {
      return false;
    }
    if (!validatePrincipalLogin()) {
      System.err.format("Principal login test failed.");
      return false;
    }
    return true;
  }

  private boolean validatePrincipalLogin() {
    // Check whether can login with specified principal and keytab
    String principal = Configuration.get(PRINCIPAL_MAP.get(mProcess));
    String keytab = Configuration.get(KEYTAB_MAP.get(mProcess));
    String primary;
    String instance;
    String realm;

    Matcher matchPrincipal = PRINCIPAL_PATTERN.matcher(principal);
    if (!matchPrincipal.matches()) {
      System.err.format("Principal %s is not in the right format.%n", principal);
      return false;
    }
    primary = matchPrincipal.group("primary");
    instance = matchPrincipal.group("instance");
    realm = matchPrincipal.group("realm");

    // Login with principal and keytab
    int exitVal =
        Utils.getResultFromProcess(new String[] {"kinit", "-kt", keytab, principal}).getExitValue();
    if (exitVal != 0) {
      System.err.format("Kerberos login failed for %s with keytab %s with exit value %d.%n",
          principal, keytab, exitVal);
      System.err.format("Primary is %s, instance is %s and realm is %s.%n",
          primary, instance, realm);
      return false;
    }
    return true;
  }
}
