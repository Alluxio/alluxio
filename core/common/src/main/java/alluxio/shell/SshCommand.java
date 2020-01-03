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

package alluxio.shell;

import alluxio.util.ShellUtils;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Object representation of a remote shell command by SSH.
 */
@NotThreadSafe
public class SshCommand extends ShellCommand {
  private final String mHostName;

  /**
   * Creates a SshCommand instance from the remote hostname and command.
   *
   * @param hostname the remote target hostname
   * @param execString the command to execute
   */
  public SshCommand(String hostname, String[] execString) {
    super(new String[]{"bash", "-c",
            String.format("ssh %s %s %s", ShellUtils.COMMON_SSH_OPTS, hostname,
                    String.join(" ", execString))});
    mHostName = hostname;
  }

  /**
   * Returns the remote target hostname.
   *
   * @return target hostname
   */
  public String getHostName() {
    return mHostName;
  }
}

