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

import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

/**
 * Task for validating security configurations.
 */
public class SecurityValidationTask implements ValidationTask {
    private static final String ALLUXIO_BIN = "./bin/alluxio";
    private static final String GETCONF_CMD = "getConf";

    public SecurityValidationTask(){
    }
    @Override
    public boolean validate() {
        String authType = getOutputFromProcess(new String[] {ALLUXIO_BIN, GETCONF_CMD,
            PropertyKey.SECURITY_AUTHENTICATION_TYPE.getName()});
        String masterPrinc = getOutputFromProcess(new String[] {ALLUXIO_BIN, GETCONF_CMD,
            PropertyKey.MASTER_PRINCIPAL.getName()});
        String masterKeytab = getOutputFromProcess(new String[] {ALLUXIO_BIN, GETCONF_CMD,
            PropertyKey.MASTER_KEYTAB_KEY_FILE.getName()});
        String workerPrinc = getOutputFromProcess(new String[] {ALLUXIO_BIN, GETCONF_CMD,
            PropertyKey.WORKER_PRINCIPAL.getName()});
        String workerKeytab = getOutputFromProcess(new String[] {ALLUXIO_BIN, GETCONF_CMD,
            PropertyKey.WORKER_KEYTAB_FILE.getName()});
        if (authType.equals("KERBEROS")) {
            System.err.format("Alluxio does not support KERBEROS authentication.%n");
            return false;
        }
        if (masterPrinc.isEmpty() && workerPrinc.isEmpty()) {
            System.out.println("Alluxio is not using Kerberos authentication"
                + " because both master and worker principals are empty.");
            return true;
        } else if (masterPrinc.isEmpty() && !workerPrinc.isEmpty()
            || !masterPrinc.isEmpty() && workerPrinc.isEmpty()) {
            System.err.format("Master principal is %s, but worker principal is %s.%n",
                masterPrinc, workerPrinc);
            return false;
        } else {
            // Login with master principal
        }
        return true;
    }

    private String getOutputFromProcess(String[] args) {
        try {
            Process process = Runtime.getRuntime().exec(args);
            try (BufferedReader processOutputReader = new BufferedReader(
                new InputStreamReader(process.getInputStream()))) {
                String line = processOutputReader.readLine();
                if (line==null) {
                    return "";
                }
                return line.trim();
            }
        } catch (IOException e) {
            StringBuilder sb = new StringBuilder();
            for (String s : args) {
                sb.append(s);
                sb.append(" ");
            }
            System.err.format("Unable to run command %s.%n", sb.toString().trim());
            return null;
        }
    }
}
