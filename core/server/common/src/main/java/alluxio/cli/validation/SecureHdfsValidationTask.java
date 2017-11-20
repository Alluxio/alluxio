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

import alluxio.PropertyKey;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Task for validating security configurations.
 */
public class SecureHdfsValidationTask implements ValidationTask {
  private static final String LINE_SEPARATOR = System.getProperty("line.separator").toString();
  private static final String ALLUXIO_BIN = "./bin/alluxio";
  private static final String GETCONF_CMD = "getConf";
  private static final Pattern PRINCIPAL_PATTERN =
      Pattern.compile("(?<primary>[\\w][\\w-]*\\$?)(/(?<instance>[\\w]+))?(@(?<realm>[\\w]+))?");

  private final String mProcess;

  /**
   * Constructor of {@link SecureHdfsValidationTask}.
   *
   * @param process type of the process on behalf of which this validation task is run
   */
  public SecureHdfsValidationTask(String process) {
    mProcess = process.toLowerCase();
  }

  @Override
  public boolean validate() {
    // Check whether can login with specified principal and keytab
    String principal = "";
    String keytab = "";
    String primary;
    String instance;
    String realm;
    if (mProcess.equals("master")) {
      principal = getResultFromProcess(new String[] {ALLUXIO_BIN, GETCONF_CMD,
          PropertyKey.MASTER_PRINCIPAL.getName()}).getOutput();
      keytab = getResultFromProcess(new String[] {ALLUXIO_BIN, GETCONF_CMD,
          PropertyKey.MASTER_KEYTAB_KEY_FILE.getName()}).getOutput();
    } else if (mProcess.equals("worker")) {
      principal = getResultFromProcess(new String[] {ALLUXIO_BIN, GETCONF_CMD,
          PropertyKey.WORKER_PRINCIPAL.getName()}).getOutput();
      keytab = getResultFromProcess(new String[] {ALLUXIO_BIN, GETCONF_CMD,
          PropertyKey.WORKER_KEYTAB_FILE.getName()}).getOutput();
    }
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
        getResultFromProcess(new String[] {"kinit", "-kt", keytab, principal}).getExitValue();
    if (exitVal != 0) {
      System.err.format("Kerberos login failed for %s with keytab %s with exit value %d.%n",
          principal, keytab, exitVal);
      System.err.format("Primary is %s, instance is %s and realm is %s.%n",
          primary, instance, realm);
      return false;
    }
    return true;
  }

  private ProcessExecutionResult getResultFromProcess(String[] args) {
    try {
      Process process = Runtime.getRuntime().exec(args);
      StringBuilder outputSb = new StringBuilder();
      try (BufferedReader processOutputReader = new BufferedReader(
          new InputStreamReader(process.getInputStream()))) {
        String line;
        while ((line = processOutputReader.readLine()) != null) {
          outputSb.append(line);
          outputSb.append(LINE_SEPARATOR);
        }
      }
      StringBuilder errorSb = new StringBuilder();
      try (BufferedReader processErrorReader = new BufferedReader(
          new InputStreamReader(process.getErrorStream()))) {
        String line;
        while ((line = processErrorReader.readLine()) != null) {
          errorSb.append(line);
          errorSb.append(LINE_SEPARATOR);
        }
      }
      process.waitFor();
      return new ProcessExecutionResult(process.exitValue(), outputSb.toString().trim(),
          errorSb.toString().trim());
    } catch (IOException e) {
      System.err.println("Failed to execute command.");
      return new ProcessExecutionResult(1, "", "");
    } catch (InterruptedException e) {
      System.err.println("Interrupted.");
      Thread.currentThread().interrupt();
      return new ProcessExecutionResult(1, "", "");
    }
  }

  static class ProcessExecutionResult {
    private final int mExitValue;
    private final String mOutput;
    private final String mError;

    public ProcessExecutionResult(int val, String output, String error) {
      mExitValue = val;
      mOutput = output;
      mError = error;
    }

    public int getExitValue() {
      return mExitValue;
    }

    public String getOutput() {
      return mOutput;
    }

    public String getError() {
      return mError;
    }
  }
}
